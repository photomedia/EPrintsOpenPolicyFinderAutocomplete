#!/usr/bin/perl
use strict;
use warnings;
use utf8;

use JSON qw(decode_json encode_json);
use File::Slurp qw(read_file write_file);
use LWP::UserAgent;
use URI::Escape qw(uri_escape);

# -------------------------------
# CONFIGURATION
# -------------------------------

my $api_key = 'ENTER YOUR API KEY HERE';

my $api_base_ids   = 'https://api.openpolicyfinder.jisc.ac.uk/object_ids';
my $api_base_by_id = 'https://api.openpolicyfinder.jisc.ac.uk/retrieve_by_id';

my $out_file       = 'romeo_journals.autocomplete';
my $construct_file = 'romeo_journals.construct';
my $cache_file     = 'journal_modified_cache.json';

my $repositoryName = 'ENTER YOUR REPOSITORY NAME HERE';

my $id_step          = 500;
my $id_fallback_step = 100;
my $max_retry        = 3;

# =====================
# INIT
# =====================

binmode STDOUT, ':encoding(UTF-8)';
binmode STDERR, ':encoding(UTF-8)';

sub trace {
    my ($msg) = @_;
    print STDERR "[TRACE] $msg\n";
}

trace('Starting script');

my $ua = LWP::UserAgent->new(
    timeout    => 30,
    agent      => 'EPrints-OpenPolicyFinder/1.0',
    keep_alive => 0,
);

$ua->default_header(
    'x-api-key' => $api_key,
    'Accept'    => 'application/json',
);

trace('HTTP client configured');

# -------------------------------
# LOAD CACHE
# -------------------------------

my %cache;
if (-e $cache_file) {
    trace("Loading cache file $cache_file");
    my $raw = read_file($cache_file, binmode => ':raw');
    if (defined $raw && length $raw) {
        my $decoded = eval { decode_json($raw) };
        if ($decoded && ref $decoded eq 'HASH') {
            %cache = %{$decoded};
        }
        else {
            warn "Cache file exists but could not be decoded cleanly; starting with empty cache\n";
        }
    }
    trace('Loaded ' . scalar(keys %cache) . ' cached entries');
}
else {
    trace('No cache file present');
}

open my $OUT, '>:encoding(UTF-8)', $construct_file
    or die "Cannot write $construct_file: $!";

trace("Opened construct file for writing");

my $total_seen       = 0;
my $total_updated    = 0;
my $total_skipped    = 0;
my $total_deleted    = 0;
my $total_requests   = 0;
my $total_live_ids   = 0;
my $start_time       = time;

# =====================
# HTTP HELPERS
# =====================

sub get_response_with_retries {
    my ($url) = @_;

    my $tries = 0;
    my $last_status = '';

    while ($tries < $max_retry) {
        my $res = $ua->get($url);
        $total_requests++;

        if ($res->is_success) {
            return {
                ok     => 1,
                res    => $res,
                status => $res->code,
            };
        }

        $last_status = $res->code || '';
        warn 'HTTP error for ' . $url . ': ' . $res->status_line
           . ' (attempt ' . ($tries + 1) . ")\n";

        sleep 2 ** $tries;
        $tries++;
    }

    return {
        ok     => 0,
        status => $last_status,
    };
}

sub get_json_with_retries {
    my ($url) = @_;

    my $tries = 0;
    while ($tries < $max_retry) {
        my $res = $ua->get($url);
        $total_requests++;

        if ($res->is_success) {
            my $raw = $res->content;
            $raw =~ s/^\x{FEFF}//;  # strip BOM if present

            my $json = eval { decode_json($raw) };
            return $json if $json && ref $json;

            warn "JSON decode error for $url (attempt " . ($tries + 1) . ")\n";
            my $sample = $res->decoded_content;
            $sample =~ s/\s+/ /g;
            $sample = substr($sample, 0, 300);
            warn "Response starts with: $sample\n";
        }
        else {
            warn 'HTTP error for ' . $url . ': ' . $res->status_line
               . ' (attempt ' . ($tries + 1) . ")\n";
        }

        sleep 2 ** $tries;
        $tries++;
    }

    return;
}

# =====================
# OBJECT_IDS PARSING
# =====================

sub extract_id_modified_rows_from_json {
    my ($json) = @_;
    my @rows;

    for my $container_name (qw/items results object_ids ids/) {
        my $container = $json->{$container_name};
        next unless defined $container;

        if (ref $container eq 'ARRAY') {
            foreach my $entry (@$container) {
                next unless ref $entry eq 'HASH';

                my $id =
                       $entry->{id}
                    // $entry->{identifier}
                    // (ref $entry->{system_metadata} eq 'HASH' ? $entry->{system_metadata}{id} : undef);

                my $modified =
                       $entry->{date_modified}
                    // $entry->{last_modified}
                    // $entry->{modified}
                    // (ref $entry->{system_metadata} eq 'HASH' ? $entry->{system_metadata}{date_modified} : undef)
                    // '';

                next unless defined $id && $id ne '';
                push @rows, { id => $id, modified => $modified };
            }

            return @rows if @rows;
        }
        elsif (ref $container eq 'HASH') {
            foreach my $id (keys %$container) {
                my $v = $container->{$id};
                my $modified = '';

                if (!ref $v) {
                    $modified = defined $v ? $v : '';
                }
                elsif (ref $v eq 'HASH') {
                    $modified =
                           $v->{date_modified}
                        // $v->{last_modified}
                        // $v->{modified}
                        // (ref $v->{system_metadata} eq 'HASH' ? $v->{system_metadata}{date_modified} : undef)
                        // '';
                }

                next unless defined $id && $id ne '';
                push @rows, { id => $id, modified => $modified };
            }

            return @rows if @rows;
        }
    }

    return @rows;
}

sub extract_id_modified_rows_from_text {
    my ($text) = @_;
    my @rows;

    return @rows unless defined $text && length $text;

    $text =~ s/\r\n/\n/g;
    $text =~ s/\r/\n/g;

    foreach my $line (split /\n/, $text) {
        next unless defined $line;
        $line =~ s/^\s+//;
        $line =~ s/\s+$//;
        next if $line eq '';

        next if $line =~ /^</;
        next if $line =~ /^id[\t,]/i;
        next if $line =~ /^identifier[\t,]/i;

        my ($id, $modified);

        if ($line =~ /\t/) {
            ($id, $modified) = split /\t+/, $line, 3;
        }
        elsif ($line =~ /,/) {
            ($id, $modified) = split /,/, $line, 3;
        }
        else {
            ($id) = split /\s+/, $line, 2;
            $modified = '';
        }

        next unless defined $id && $id ne '';
        $modified = '' unless defined $modified;

        push @rows, { id => $id, modified => $modified };
    }

    return @rows;
}

sub get_object_ids_rows {
    my ($url) = @_;

    my $response_info = get_response_with_retries($url);
    unless ($response_info->{ok}) {
        return {
            ok     => 0,
            error  => 'http_failure',
            status => $response_info->{status},
            rows   => [],
        };
    }

    my $res = $response_info->{res};
    my $raw = $res->content;
    $raw =~ s/^\x{FEFF}//;

    my $json = eval { decode_json($raw) };
    if ($json && ref $json) {
        my @rows = extract_id_modified_rows_from_json($json);
        return {
            ok     => 1,
            status => 200,
            rows   => \@rows,
        } if @rows;
    }

    my $text = $res->decoded_content;
    my @rows = extract_id_modified_rows_from_text($text);
    return {
        ok     => 1,
        status => 200,
        rows   => \@rows,
    } if @rows;

    warn "Could not parse object_ids response at $url\n";
    my $sample = $text;
    $sample =~ s/\s+/ /g;
    $sample = substr($sample, 0, 300);
    warn "Response starts with: $sample\n";

    return {
        ok     => 1,
        status => 200,
        rows   => [],
    };
}

# =====================
# RETRIEVE_BY_ID PARSING
# =====================

sub extract_single_item {
    my ($json) = @_;

    for my $container_name (qw/items results/) {
        my $container = $json->{$container_name};
        if (ref $container eq 'ARRAY' && @$container && ref $container->[0] eq 'HASH') {
            return $container->[0];
        }
    }

    if (ref $json eq 'HASH') {
        if (exists $json->{id} || exists $json->{title} || exists $json->{system_metadata}) {
            return $json;
        }
    }

    return;
}

# =====================
# FETCH LIVE IDS + MODIFIED DATES
# =====================

trace('Fetching live object IDs');
my @live_rows;
my %live_lookup;
my $offset = 0;
my $current_id_limit = $id_step;
my $last_successful_page_count = 0;

while (1) {
    trace("Requesting object_ids offset=$offset");

    my $url = $api_base_ids
            . '?item-type=publication'
            . '&limit='  . $current_id_limit
            . '&offset=' . $offset;

    my $result = get_object_ids_rows($url);

    if (!$result->{ok}) {
        if (
            ($result->{status} || '') == 500
            && $last_successful_page_count == $current_id_limit
            && $current_id_limit > $id_fallback_step
        ) {
            trace("Retrying offset=$offset with smaller limit=$id_fallback_step after 500");
            $current_id_limit = $id_fallback_step;
            next;
        }

        warn "Could not fetch object_ids page at offset $offset\n";
        last;
    }

    my @rows = @{ $result->{rows} || [] };
    last unless @rows;

    foreach my $row (@rows) {
        my $id = $row->{id};
        next if exists $live_lookup{$id};   # defensive de-dup
        push @live_rows, $row;
        $live_lookup{$id} = $row->{modified};
    }

    $last_successful_page_count = scalar(@rows);

    # Fix 1: stop on a short successful final page
    last if $last_successful_page_count < $current_id_limit;

    $offset += $current_id_limit;
}

$total_live_ids = scalar(@live_rows);
trace("Fetched $total_live_ids live IDs");

# -------------------------------
# PRUNE DELETED CACHE ENTRIES
# -------------------------------

foreach my $cached_id (keys %cache) {
    next if exists $live_lookup{$cached_id};
    delete $cache{$cached_id};
    $total_deleted++;
}

trace("Pruned $total_deleted deleted cache entries");

# =====================
# MAIN BUILD LOOP
# =====================

foreach my $row (@live_rows) {
    my $id       = $row->{id};
    my $modified = $row->{modified} // '';

    $total_seen++;

    if (
        exists $cache{$id}
        && ref $cache{$id} eq 'HASH'
        && ($cache{$id}->{modified} // '') eq $modified
        && defined $cache{$id}->{html}
        && $cache{$id}->{html} ne ''
    ) {
        print {$OUT} $cache{$id}->{html};
        $total_skipped++;
        next;
    }

    trace("Refreshing id=$id");

    my $url = $api_base_by_id
            . '?item-type=publication'
            . '&format=Json'
            . '&identifier=' . uri_escape($id);

    my $json = get_json_with_retries($url);
    unless ($json) {
        warn "Could not fetch record for id=$id\n";
        next;
    }

    my $item = extract_single_item($json);
    unless ($item && ref $item eq 'HASH') {
        warn "Could not extract single item for id=$id\n";
        next;
    }

    my $html = processJournal($item, $repositoryName);
    next unless defined $html && $html ne '';

    $cache{$id} = {
        modified => $modified,
        html     => $html,
    };

    print {$OUT} $html;
    $total_updated++;

    if (($total_seen % 500) == 0) {
        my $elapsed = time - $start_time;
        printf STDERR
            "[%s] seen=%d live_ids=%d updated=%d skipped=%d deleted=%d requests=%d elapsed=%ds\n",
            scalar localtime(),
            $total_seen,
            $total_live_ids,
            $total_updated,
            $total_skipped,
            $total_deleted,
            $total_requests,
            $elapsed;
    }
}

# =====================
# FINALIZE
# =====================

close $OUT;

trace('Finalizing run');

my $cache_tmp = "$cache_file.tmp";
write_file(
    $cache_tmp,
    { binmode => ':raw' },
    encode_json(\%cache)
);

rename $cache_tmp, $cache_file
    or die "Cache rename failed: $!";

rename $construct_file, $out_file
    or die "Rename failed: $!";

trace('Run complete');

print STDERR <<"SUMMARY";
Done.
Live IDs: $total_live_ids
Requests made: $total_requests
Items seen: $total_seen
Items updated: $total_updated
Items skipped (cache): $total_skipped
Items deleted from cache: $total_deleted
SUMMARY

# =====================
# JOURNAL PROCESSOR
# (output preserved from original script)
# =====================

sub contains_one_of {
    my ($strings, $matches) = @_;
    return 0 unless ref $strings eq 'ARRAY';
    return 0 unless ref $matches eq 'ARRAY';

    foreach my $string (@$strings) {
        return 1 if grep { $_ eq $string } @$matches;
    }
    return 0;
}

sub uniq {
    my %seen;
    return grep { !$seen{$_}++ } @_;
}

sub first_title {
    my ($title_arrayref) = @_;
    return unless ref $title_arrayref eq 'ARRAY' && @$title_arrayref;

    my $fallback;
    foreach my $t (@$title_arrayref) {
        next unless ref $t eq 'HASH';
        if (($t->{language} // '') eq 'en' && defined $t->{title}) {
            return $t->{title};
        }
        $fallback = $t->{title} if !defined $fallback && defined $t->{title};
    }
    return $fallback;
}

sub processJournal {
    my ($item, $repositoryName) = @_;

    my $title = first_title($item->{title});
    my $issn;
    my $issn2;

    if (ref $item->{issns} eq 'ARRAY' && @{$item->{issns}}) {
        $issn  = $item->{issns}[0]{issn} if ref $item->{issns}[0] eq 'HASH';
        $issn2 = $item->{issns}[1]{issn}
            if @{$item->{issns}} > 1 && ref $item->{issns}[1] eq 'HASH';
    }

    my $publisher_id   = '';
    my $publisher_name = '';

    if (
        ref $item->{publishers} eq 'ARRAY'
        && @{$item->{publishers}}
        && ref $item->{publishers}[0] eq 'HASH'
        && ref $item->{publishers}[0]{publisher} eq 'HASH'
    ) {
        my $publisher = $item->{publishers}[0]{publisher};
        $publisher_id = $publisher->{id} // '';
        if (ref $publisher->{name} eq 'ARRAY' && @{$publisher->{name}}) {
            $publisher_name = $publisher->{name}[0]{name} // '';
        }
    }

    my $publication_id = $item->{id};

    return unless defined $title && $title ne '';
    return unless defined $publisher_id && $publisher_id ne '';

    my $return_str = $title . "\t";

    my $permitted_oa_versions = '';
    my $versions = 'version';

    my @PolicySummary;
    my @unique_versions;
    my $i = 0;

    my $interesting_locations = [
        qw/
          institutional_repository
          any_website
          non_commercial_repository
          non_commercial_institutional_repository
          any_repository
        /
    ];

    my $publisher_policy = [];
    if (ref $item->{publisher_policy} eq 'ARRAY') {
        $publisher_policy = $item->{publisher_policy};
    }

    my $permitted_oa = [];
    if (
        @$publisher_policy
        && ref $publisher_policy->[0] eq 'HASH'
        && ref $publisher_policy->[0]{permitted_oa} eq 'ARRAY'
    ) {
        $permitted_oa = $publisher_policy->[0]{permitted_oa};
    }

    foreach my $temp (@$permitted_oa) {
        next unless ref $temp eq 'HASH';

        my $locations = [];
        if (ref $temp->{location} eq 'HASH' && ref $temp->{location}{location} eq 'ARRAY') {
            $locations = $temp->{location}{location};
        }

        next unless contains_one_of($locations, $interesting_locations);

        if (($temp->{additional_oa_fee} // '') eq 'no') {

            foreach my $ver (@{ $temp->{article_version} // [] }) {
                push @unique_versions, $ver if defined $ver && $ver ne '';
            }

            my $conditions = join('', map { '<li>' . $_ . '</li>' } @{ $temp->{conditions} // [] });

            my $prereq = '';
            my $prereq_funders = '';
            my $prereq_subjects = '';

            if (ref $temp->{prerequisites} eq 'HASH') {
                $prereq = join(
                    '',
                    map { '<li>' . $_ . '</li>' }
                    @{ $temp->{prerequisites}{prerequisites} // [] }
                );

                $prereq_funders = join(
                    '',
                    map {
                        my $name = '';
                        if (
                            ref $_ eq 'HASH'
                            && ref $_->{funder_metadata} eq 'HASH'
                            && ref $_->{funder_metadata}{name} eq 'ARRAY'
                            && @{ $_->{funder_metadata}{name} }
                        ) {
                            $name = $_->{funder_metadata}{name}[0]{name} // '';
                        }
                        $name ne '' ? '<li>' . $name . '</li>' : ()
                    }
                    @{ $temp->{prerequisites}{prerequisite_funders} // [] }
                );

                $prereq_subjects = join(
                    '',
                    map { '<li>' . $_ . '</li>' }
                    @{ $temp->{prerequisites}{prerequisite_subjects} // [] }
                );
            }

            my $embargo = '';
            if (ref $temp->{embargo} eq 'HASH') {
                my $amount = $temp->{embargo}{amount} // '';
                my $units  = $temp->{embargo}{units}  // '';
                $embargo = $amount . ' ' . $units if $amount ne '' || $units ne '';
                $embargo =~ s/\s+$//;
            }

            my $license = '';
            if (ref $temp->{license} eq 'ARRAY' && @{ $temp->{license} }) {
                $license = $temp->{license}[0]{license} // '';
            }

            $PolicySummary[$i] = [
                join(', ', @{ $temp->{article_version} // [] }),
                $conditions,
                $embargo,
                $prereq,
                $prereq_funders,
                $prereq_subjects,
                '',
                $license,
            ];

        }
        else {
            $PolicySummary[$i] = [('', '', '', '', '', '', 'with_oa_fee', '')];
        }
        $i++;
    }

    my @uniq_versions = uniq(@unique_versions);
    $permitted_oa_versions = join(', ', @uniq_versions);
    $versions = 'versions' if scalar(@uniq_versions) > 1;

    my $color = $permitted_oa_versions ? '#dfeccf' : '#f0f0f0';
    $return_str .= "<li style='border-right: solid 50px $color'>";
    $return_str .= "$title published by $publisher_name<br />";
    $return_str .= $permitted_oa_versions
        ? "<small>$permitted_oa_versions $versions can be archived in $repositoryName.</small>"
        : "<small>No version can be archived in $repositoryName.</small>";

    $return_str .= "<ul><li id=\"for:value:component:_publication\">$title</li><li id=\"for:value:component:_publisher\">$publisher_name</li>";
    $return_str .= '<li id="for:value:component:_issn">' . $issn . '</li>' if defined $issn && !defined $issn2;
    $return_str .= '<li id="for:block:absolute:publisher_policy"><a href="https://openpolicyfinder.jisc.ac.uk/" target="_new"><img src="/style/images/OPF.png" style="float: right; padding-right: 1em"/></a>Journal autocompletion information is derived from the <a href="https://openpolicyfinder.jisc.ac.uk/" target="_new">Open Policy Finder</a> database, an online resource that aggregates and analyses publisher open access policies.<p> This publication, <a title="Link to the publication information on Open Policy Finder" target="_new" href="https://openpolicyfinder.jisc.ac.uk/id/publication/' . $publication_id . '">' . $title . '</a>, is published by <a target="_new" title="Link to the publisher information on Open Policy Finder" href="https://openpolicyfinder.jisc.ac.uk/id/publisher/' . $publisher_id . '">' . $publisher_name . '</a>.</p>';
    if ($permitted_oa_versions) {
        $return_str .= "The depositor can archive $permitted_oa_versions $versions without additional Open Access fees.";
    }
    else {
        my $possible_with_fee = grep { $_->[6] eq 'with_oa_fee' } @PolicySummary;
        $return_str .= $possible_with_fee
            ? "The depositor cannot archive any version in $repositoryName without an additional open access fee to the publisher."
            : "The depositor cannot archive any version in $repositoryName.";
    }

    for my $policy (@PolicySummary) {
        next if $policy->[6] eq 'with_oa_fee';
        if ($policy->[1] ne '' || $policy->[2] || $policy->[3] || $policy->[4] || $policy->[5] || $policy->[7]) {
            $return_str .= "<p>";
            $return_str .= "The publisher also defines the following conditions for deposit of $policy->[0] version:<ul>";
            $return_str .= "<li>Required license of deposit: $policy->[7]</li>" if $policy->[7];
            $return_str .= "<li>Prerequisite condition(s):<ul>$policy->[3]</ul></li>" if $policy->[3];
            $return_str .= "<li>Prerequisite funder(s):<ul>$policy->[4]</ul></li>" if $policy->[4];
            $return_str .= "<li>Prerequisite subject(s):<ul>$policy->[5]</ul></li>" if $policy->[5];
            $return_str .= "<li>Embargo period of $policy->[2]</li>" if $policy->[2];
            $return_str .= $policy->[1] if $policy->[1];
            $return_str .= "</ul></p>";
        }
    }

    $return_str .= "</li></ul></li>\n";
    return $return_str;
}