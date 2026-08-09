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

# Post-July 2026 OpenPolicyFinder API platform.
# API keys must be sent in the x-api-key request header.
my $api_host       = 'https://api.openpolicyfinder.jisc.ac.uk';
my $api_base_ids   = "$api_host/object_ids";
my $api_base_by_id = "$api_host/retrieve_by_id";

my $out_file       = '/opt/eprints3/archives/library/cfg/autocomplete/romeo_journals.autocomplete';
my $construct_file = '/opt/eprints3/archives/library/cfg/autocomplete/romeo_journals.construct';
my $cache_file     = '/opt/eprints3/archives/library/cfg/autocomplete/journal_modified_cache.json';

my $repositoryName = 'ENTER YOUR REPOSITORY NAME HERE';

my $id_step          = 500;
my $id_fallback_step = 100;
my $max_retry        = 3;

# Optional quick refresh mode:
#   ./get_journal_titles_policies.pl --quick-id 18195
#   ./get_journal_titles_policies.pl 18195
#
# This refreshes one publication ID, updates the cache, patches the
# autocomplete file, and exits without walking the full dataset.
my $quick_refresh_id = '';

if (@ARGV) {
    if (($ARGV[0] // '') eq '--quick-id' && defined $ARGV[1]) {
        $quick_refresh_id = $ARGV[1];
    }
    elsif (($ARGV[0] // '') =~ /^\d+$/) {
        $quick_refresh_id = $ARGV[0];
    }

    if ($quick_refresh_id ne '') {
        $quick_refresh_id =~ s/^\s+//;
        $quick_refresh_id =~ s/\s+$//;
    }
}

# =====================
# INIT
# =====================

binmode STDOUT, ':encoding(UTF-8)';
binmode STDERR, ':encoding(UTF-8)';

sub trace {
    my ($msg) = @_;
    print STDERR "[TRACE] $msg\n";
}

sub html_escape {
    my ($value) = @_;
    return '' unless defined $value;
    $value =~ s/&/&amp;/g;
    $value =~ s/</&lt;/g;
    $value =~ s/>/&gt;/g;
    $value =~ s/"/&quot;/g;
    $value =~ s/'/&#39;/g;
    return $value;
}

sub build_url {
    my ($base, $params) = @_;
    my @pairs;
    foreach my $key (sort keys %$params) {
        next unless defined $params->{$key};
        push @pairs, uri_escape($key) . '=' . uri_escape($params->{$key});
    }
    return $base . '?' . join('&', @pairs);
}

sub metadata_id {
    my ($obj) = @_;
    return '' unless ref $obj eq 'HASH';
    return $obj->{id} if defined $obj->{id} && $obj->{id} ne '';
    return $obj->{system_metadata}{id}
        if ref $obj->{system_metadata} eq 'HASH'
        && defined $obj->{system_metadata}{id}
        && $obj->{system_metadata}{id} ne '';
    return '';
}

trace('Starting script');

if ($quick_refresh_id ne '') {
    trace("Quick refresh requested for id=$quick_refresh_id");
}

my $ua = LWP::UserAgent->new(
    timeout    => 30,
    agent      => 'EPrints-OpenPolicyFinder/2.0',
    keep_alive => 0,
);

$ua->default_header(
    'x-api-key' => $api_key,
    'Accept'    => 'application/json, text/json, text/csv, text/plain',
);

trace('HTTP client configured for OpenPolicyFinder API platform');

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
            return { ok => 1, res => $res, status => $res->code };
        }
        $last_status = $res->code || '';
        warn 'HTTP error for ' . $url . ': ' . $res->status_line
           . ' (attempt ' . ($tries + 1) . ")\n";
        sleep 2 ** $tries;
        $tries++;
    }
    return { ok => 0, status => $last_status };
}

sub get_json_with_retries {
    my ($url) = @_;
    my $tries = 0;

    while ($tries < $max_retry) {
        my $res = $ua->get($url);
        $total_requests++;
        if ($res->is_success) {
            my $raw = $res->decoded_content;
            $raw =~ s/^\x{FEFF}// if defined $raw;
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
    return @rows unless $json && ref $json;

    my @entries;
    if (ref $json eq 'ARRAY') {
        @entries = @$json;
    }
    elsif (ref $json eq 'HASH') {
        for my $container_name (qw/items results object_ids ids data/) {
            my $container = $json->{$container_name};
            next unless defined $container;
            if (ref $container eq 'ARRAY') {
                @entries = @$container;
                last;
            }
            elsif (ref $container eq 'HASH') {
                foreach my $id (keys %$container) {
                    my $v = $container->{$id};
                    my $modified = '';
                    if (!ref $v) {
                        $modified = defined $v ? $v : '';
                    }
                    elsif (ref $v eq 'HASH') {
                        $modified = $v->{date_modified}
                            // $v->{last_modified}
                            // $v->{modified}
                            // ((ref $v->{system_metadata} eq 'HASH') ? $v->{system_metadata}{date_modified} : '')
                            // '';
                    }
                    next unless defined $id && $id ne '';
                    push @rows, { id => "$id", modified => "$modified" };
                }
                return @rows if @rows;
            }
        }
    }

    foreach my $entry (@entries) {
        next unless ref $entry eq 'HASH';
        my $system_metadata = ref $entry->{system_metadata} eq 'HASH' ? $entry->{system_metadata} : {};
        my $id = $system_metadata->{id}
            // $entry->{id}
            // $entry->{identifier}
            // $entry->{object_id};
        my $modified = $system_metadata->{date_modified}
            // $entry->{date_modified}
            // $entry->{last_modified}
            // $entry->{modified}
            // '';
        next unless defined $id && $id ne '';
        push @rows, { id => "$id", modified => "$modified" };
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
        push @rows, { id => "$id", modified => "$modified" };
    }
    return @rows;
}

sub get_object_ids_rows {
    my ($url) = @_;
    my $response_info = get_response_with_retries($url);
    unless ($response_info->{ok}) {
        return { ok => 0, error => 'http_failure', status => $response_info->{status}, rows => [] };
    }

    my $res = $response_info->{res};
    my $raw = $res->decoded_content;
    $raw =~ s/^\x{FEFF}// if defined $raw;
    my $json = eval { decode_json($raw) };
    if ($json && ref $json) {
        my @rows = extract_id_modified_rows_from_json($json);
        return { ok => 1, status => 200, rows => \@rows } if @rows;
    }

    my $text = $res->decoded_content;
    my @rows = extract_id_modified_rows_from_text($text);
    return { ok => 1, status => 200, rows => \@rows } if @rows;

    warn "Could not parse object_ids response at $url\n";
    my $sample = $text;
    $sample =~ s/\s+/ /g;
    $sample = substr($sample, 0, 300);
    warn "Response starts with: $sample\n";
    return { ok => 1, status => 200, rows => [] };
}

# =====================
# RETRIEVE_BY_ID PARSING
# =====================

sub extract_single_item {
    my ($json) = @_;
    return unless $json && ref $json;

    if (ref $json eq 'ARRAY') {
        foreach my $entry (@$json) {
            return $entry if ref $entry eq 'HASH';
        }
        return;
    }

    if (ref $json eq 'HASH') {
        for my $container_name (qw/items results data/) {
            my $container = $json->{$container_name};
            if (ref $container eq 'ARRAY' && @$container) {
                foreach my $entry (@$container) {
                    return $entry if ref $entry eq 'HASH';
                }
            }
            elsif (ref $container eq 'HASH') {
                return $container;
            }
        }
        return $json if exists $json->{id} || exists $json->{title} || exists $json->{system_metadata} || exists $json->{publisher_policy};
    }
    return;
}


sub quick_refresh_one_id {
    my ($id) = @_;

    die "No ID supplied for quick refresh\n"
        unless defined $id && $id ne '';

    trace("Quick refresh mode enabled for id=$id");

    my $url = build_url(
        $api_base_by_id,
        {
            'item-type'  => 'publication',
            'identifier' => $id,
        }
    );

    my $json = get_json_with_retries($url);

    die "Could not fetch record for quick refresh id=$id\n"
        unless $json;

    my $item = extract_single_item($json);

    die "Could not extract single item for quick refresh id=$id\n"
        unless $item && ref $item eq 'HASH';
        
    warn "\n===== API JSON ITEM =====\n";
    print STDERR JSON->new->pretty->canonical->encode($item);
    warn "\n===== END API JSON ITEM =====\n";

    my $html = processJournal($item, $repositoryName);
    
    warn "\n===== GENERATED HTML =====\n";
    warn $html;
    warn "\n===== END GENERATED HTML =====\n";

    die "processJournal returned empty output for quick refresh id=$id\n"
        unless defined $html && $html ne '';

    my $modified = '';

    if (ref $item->{system_metadata} eq 'HASH') {
        $modified = $item->{system_metadata}{date_modified} // '';
    }

    $modified =
           $item->{date_modified}
        // $item->{last_modified}
        // $item->{modified}
        // $modified
        // '';

    $cache{$id} = {
        modified => "$modified",
        html     => $html,
    };

    my $cache_tmp = "$cache_file.tmp";

    write_file(
        $cache_tmp,
        { binmode => ':raw' },
        encode_json(\%cache)
    );

    rename $cache_tmp, $cache_file
        or die "Cache rename failed during quick refresh: $!";

    my ($new_title) = split /\t/, $html, 2;

    die "Could not determine title from generated autocomplete line for id=$id\n"
        unless defined $new_title && $new_title ne '';

    my @lines;

    if (-e $out_file) {
        my $existing = read_file($out_file, binmode => ':encoding(UTF-8)');
        @lines = split /\n/, $existing, -1;
    }

    my $replaced = 0;
    my $html_line = $html;
    chomp $html_line;

    for (my $i = 0; $i < @lines; $i++) {
        next if !defined $lines[$i] || $lines[$i] eq '';

        my ($line_title) = split /\t/, $lines[$i], 2;

        if (defined $line_title && $line_title eq $new_title) {
            $lines[$i] = $html_line;
            $replaced = 1;
            last;
        }
    }

    if (!$replaced) {
        push @lines, $html_line;
    }

    my $out_tmp = "$out_file.tmp";

    write_file(
        $out_tmp,
        { binmode => ':encoding(UTF-8)' },
        join("\n", grep { defined $_ && $_ ne '' } @lines) . "\n"
    );

    rename $out_tmp, $out_file
        or die "Autocomplete file rename failed during quick refresh: $!";

    trace("Quick refresh complete for id=$id");
    trace($replaced ? 'Updated existing autocomplete entry' : 'Added new autocomplete entry');

    print STDERR <<"SUMMARY";
Done quick refresh.
ID refreshed: $id
Requests made: $total_requests
Autocomplete file patched: $out_file
Cache file patched: $cache_file
SUMMARY

    exit 0;
}

if ($quick_refresh_id ne '') {
    close $OUT if defined fileno($OUT);
    unlink $construct_file if -e $construct_file;
    quick_refresh_one_id($quick_refresh_id);
}

# =====================
# FETCH LIVE IDS + MODIFIED DATES
# =====================

trace('Fetching live object IDs');

my @live_rows;
my %live_lookup;

my $search_after;
my $current_id_limit = $id_step;

while (1) {
    my %params = (
        'item-type' => 'publication',
        'limit'     => $current_id_limit,
    );

    if (defined $search_after && $search_after ne '') {
        $params{'search_after'} = $search_after;
        trace("Requesting object_ids search_after=$search_after limit=$current_id_limit");
    }
    else {
        trace("Requesting first object_ids page limit=$current_id_limit");
    }

    my $url = build_url($api_base_ids, \%params);
    my $result = get_object_ids_rows($url);

    if (!$result->{ok}) {
        if (($result->{status} || '') == 500 && $current_id_limit > $id_fallback_step) {
            trace("Retrying with smaller limit=$id_fallback_step after 500");
            $current_id_limit = $id_fallback_step;
            next;
        }

        die "Could not fetch object_ids page using search_after pagination\n";
    }

    my @rows = @{ $result->{rows} || [] };
    last unless @rows;

    foreach my $row (@rows) {
        my $id = $row->{id};
        next unless defined $id && $id ne '';
        next if exists $live_lookup{$id};

        push @live_rows, $row;
        $live_lookup{$id} = $row->{modified} // '';
    }

    my $page_count = scalar(@rows);
    trace("Fetched object_ids page with $page_count rows");

    last if $page_count < $current_id_limit;

    my $last_row = $rows[-1];
    my $last_id  = $last_row->{id};

    last unless defined $last_id && $last_id ne '';

    if (defined $search_after && $search_after eq $last_id) {
        die "Pagination did not advance: search_after remained $search_after\n";
    }

    $search_after = $last_id;
}

$total_live_ids = scalar(@live_rows);
trace("Fetched $total_live_ids live IDs");

my $cached_count = scalar(keys %cache);

if ($cached_count > 0 && $total_live_ids < int($cached_count * 0.80)) {
    die "Refusing to prune cache: live ID count $total_live_ids is suspiciously low compared with cached count $cached_count. Possible API pagination failure.\n";
}

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
    next unless defined $id && $id ne '';
    $total_seen++;

    if (exists $cache{$id} && ref $cache{$id} eq 'HASH' && ($cache{$id}->{modified} // '') eq $modified && defined $cache{$id}->{html} && $cache{$id}->{html} ne '') {
        print {$OUT} $cache{$id}->{html};
        $total_skipped++;
        next;
    }

    trace("Refreshing id=$id");
    my $url = build_url($api_base_by_id, {
        'item-type'  => 'publication',
        'identifier' => $id,
    });

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

    $cache{$id} = { modified => $modified, html => $html };
    print {$OUT} $html;
    $total_updated++;

    if (($total_seen % 500) == 0) {
        my $elapsed = time - $start_time;
        printf STDERR "[%s] seen=%d live_ids=%d updated=%d skipped=%d deleted=%d requests=%d elapsed=%ds\n",
            scalar localtime(), $total_seen, $total_live_ids, $total_updated, $total_skipped, $total_deleted, $total_requests, $elapsed;
    }
}

# =====================
# FINALIZE
# =====================

close $OUT;
trace('Finalizing run');

my $cache_tmp = "$cache_file.tmp";
write_file($cache_tmp, { binmode => ':raw' }, encode_json(\%cache));
rename $cache_tmp, $cache_file or die "Cache rename failed: $!";
rename $construct_file, $out_file or die "Rename failed: $!";

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
    return grep { defined $_ && !$seen{$_}++ } @_;
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

        $fallback = $t->{title}
            if !defined $fallback && defined $t->{title};
    }

    return $fallback;
}

sub first_name {
    my ($name_arrayref) = @_;

    return '' unless ref $name_arrayref eq 'ARRAY' && @$name_arrayref;

    my $fallback = '';

    foreach my $n (@$name_arrayref) {
        next unless ref $n eq 'HASH';

        if (($n->{language} // '') eq 'en' && defined $n->{name}) {
            return $n->{name};
        }

        $fallback = $n->{name}
            if $fallback eq '' && defined $n->{name};
    }

    return $fallback;
}

sub extract_values {
    my ($value, @preferred_keys) = @_;

    my @out;
    return @out unless defined $value;

    if (!ref $value) {
        push @out, $value if $value ne '';
    }
    elsif (ref $value eq 'ARRAY') {
        foreach my $entry (@$value) {
            push @out, extract_values($entry, @preferred_keys);
        }
    }
    elsif (ref $value eq 'HASH') {
        foreach my $key (@preferred_keys, qw/value id name title phrase license article_version location/) {
            next unless exists $value->{$key};

            my $v = $value->{$key};

            if (!ref $v) {
                push @out, $v if defined $v && $v ne '';
                last;
            }
            elsif (ref $v eq 'ARRAY' || ref $v eq 'HASH') {
                my @nested = extract_values($v, @preferred_keys);
                push @out, @nested;
                last if @nested;
            }
        }
    }

    return @out;
}

sub extract_location_values {
    my ($location_obj) = @_;

    my @locations;
    return @locations unless defined $location_obj;

    if (!ref $location_obj) {
        push @locations, $location_obj if $location_obj ne '';
    }
    elsif (ref $location_obj eq 'ARRAY') {
        foreach my $entry (@$location_obj) {
            push @locations, extract_location_values($entry);
        }
    }
    elsif (ref $location_obj eq 'HASH') {
        if (exists $location_obj->{location}) {
            push @locations, extract_location_values($location_obj->{location});
        }
        else {
            push @locations, extract_values(
                $location_obj,
                qw/location value id name title phrase/
            );
        }
    }

    return uniq(grep { defined $_ && $_ ne '' } @locations);
}

sub is_interesting_location {
    my ($locations) = @_;

    return 0 unless ref $locations eq 'ARRAY';

    # Spectrum is an institutional repository, so only count locations
    # that are compatible with repository deposit in Spectrum.
    #
    # Do not include authors_homepage, named_repository,
    # subject_repository, or non_commercial_subject_repository here.
    my %spectrum_compatible = map { $_ => 1 } qw/
        any_repository
        any_website
        non_commercial_website
        institutional_repository
        non_commercial_repository
        non_commercial_institutional_repository
        repository
    /;

    foreach my $loc (@$locations) {
        next unless defined $loc;

        my $l = lc $loc;
        return 1 if $spectrum_compatible{$l};
    }

    return 0;
}

sub has_additional_oa_fee {
    my ($value) = @_;

    # In Open Policy Finder permitted_oa records, additional_oa_fee is often
    # omitted for normal self-archiving permissions. Treat only an explicit
    # yes/true/1 value as fee-bearing. If the field is absent, it is not an
    # additional OA fee option.
    return 0 unless defined $value;

    if (ref $value eq 'HASH') {
        my @values = extract_values($value, qw/additional_oa_fee value id name phrase/);
        $value = $values[0] // '';
    }
    elsif (ref $value eq 'ARRAY') {
        my @values = extract_values($value, qw/additional_oa_fee value id name phrase/);
        $value = $values[0] // '';
    }

    my $v = lc "$value";
    $v =~ s/^\s+//;
    $v =~ s/\s+$//;

    return 1 if $v eq 'yes';
    return 1 if $v eq 'true';
    return 1 if $v eq '1';

    return 0;
}

sub processJournal {
    my ($item, $repositoryName) = @_;

    my $title = first_title($item->{title});
    return unless defined $title && $title ne '';

    my $issn;
    my $issn2;

    if (ref $item->{issns} eq 'ARRAY' && @{ $item->{issns} }) {
        $issn = $item->{issns}[0]{issn}
            if ref $item->{issns}[0] eq 'HASH';

        $issn2 = $item->{issns}[1]{issn}
            if @{ $item->{issns} } > 1
            && ref $item->{issns}[1] eq 'HASH';
    }

    my $publisher_id   = '';
    my $publisher_name = '';

    if (
        ref $item->{publishers} eq 'ARRAY'
        && @{ $item->{publishers} }
        && ref $item->{publishers}[0] eq 'HASH'
        && ref $item->{publishers}[0]{publisher} eq 'HASH'
    ) {
        my $publisher = $item->{publishers}[0]{publisher};

        $publisher_id = metadata_id($publisher);
        $publisher_name = first_name($publisher->{name});
    }

    my $publication_id = metadata_id($item);

    return unless defined $publisher_id && $publisher_id ne '';

    my $title_html          = html_escape($title);
    my $publisher_name_html = html_escape($publisher_name);
    my $publisher_id_url    = uri_escape($publisher_id);
    my $publication_id_url  = uri_escape($publication_id);
    my $repository_html     = html_escape($repositoryName);

    my $return_str = $title . "\t";

    my @PolicySummary;
    my @unique_versions;
    my $i = 0;

    my $publisher_policy = [];

    if (ref $item->{publisher_policy} eq 'ARRAY') {
        $publisher_policy = $item->{publisher_policy};
    }

    my @permitted_oa_all;

    foreach my $policy (@$publisher_policy) {
        next unless ref $policy eq 'HASH';

        if (ref $policy->{permitted_oa} eq 'ARRAY') {
            push @permitted_oa_all, @{ $policy->{permitted_oa} };
        }
    }

    foreach my $temp (@permitted_oa_all) {
        next unless ref $temp eq 'HASH';

        my @locations = extract_location_values($temp->{location});
        next unless is_interesting_location(\@locations);

        if (!has_additional_oa_fee($temp->{additional_oa_fee})) {
            my @versions = extract_values(
                $temp->{article_version},
                qw/article_version value id name phrase/
            );

            @versions = uniq(grep { defined $_ && $_ ne '' } @versions);

            foreach my $ver (@versions) {
                push @unique_versions, $ver
                    if defined $ver && $ver ne '';
            }

            my @condition_values = extract_values(
                $temp->{conditions},
                qw/condition value text description phrase/
            );

            my $conditions = join(
                '',
                map { '<li>' . html_escape($_) . '</li>' }
                @condition_values
            );

            my $prereq = '';
            my $prereq_funders = '';
            my $prereq_subjects = '';

            if (ref $temp->{prerequisites} eq 'HASH') {
                my @prereqs = extract_values(
                    $temp->{prerequisites}{prerequisites},
                    qw/prerequisite value text description phrase/
                );

                $prereq = join(
                    '',
                    map { '<li>' . html_escape($_) . '</li>' }
                    @prereqs
                );

                if (ref $temp->{prerequisites}{prerequisite_funders} eq 'ARRAY') {
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
                                $name = first_name($_->{funder_metadata}{name});
                            }
                            elsif (ref $_ eq 'HASH') {
                                my @names = extract_values($_, qw/name value title phrase/);
                                $name = $names[0] // '';
                            }

                            $name ne '' ? '<li>' . html_escape($name) . '</li>' : ();
                        }
                        @{ $temp->{prerequisites}{prerequisite_funders} }
                    );
                }

                my @subjects = extract_values(
                    $temp->{prerequisites}{prerequisite_subjects},
                    qw/subject value name title phrase/
                );

                $prereq_subjects = join(
                    '',
                    map { '<li>' . html_escape($_) . '</li>' }
                    @subjects
                );
            }

            my $embargo = '';

            if (ref $temp->{embargo} eq 'HASH') {
                my $amount = $temp->{embargo}{amount} // '';
                my $units  = $temp->{embargo}{units}  // '';

                $embargo = $amount . ' ' . $units
                    if $amount ne '' || $units ne '';

                $embargo =~ s/\s+$//;
            }

            my @licenses = extract_values(
                $temp->{license},
                qw/license value id name title phrase/
            );

            my $license = $licenses[0] // '';

            $PolicySummary[$i] = [
                html_escape(join(', ', @versions)),
                $conditions,
                html_escape($embargo),
                $prereq,
                $prereq_funders,
                $prereq_subjects,
                '',
                html_escape($license),
            ];
        }
        else {
            $PolicySummary[$i] = [('', '', '', '', '', '', 'with_oa_fee', '')];
        }

        $i++;
    }

    my @uniq_versions = uniq(@unique_versions);
    my $permitted_oa_versions = html_escape(join(', ', @uniq_versions));
    my $versions = 'version';
    $versions = 'versions' if scalar(@uniq_versions) > 1;

    my $color = $permitted_oa_versions ? '#dfeccf' : '#f0f0f0';

    $return_str .= "<li style='border-right: solid 50px $color'>";
    $return_str .= "$title_html published by $publisher_name_html<br />";

    $return_str .= $permitted_oa_versions
        ? "<small>$permitted_oa_versions $versions can be archived in $repository_html.</small>"
        : "<small>No version can be archived in $repository_html.</small>";

    $return_str .= '<ul>';
    $return_str .= '<li id="for:value:component:_publication">' . $title_html . '</li>';
    $return_str .= '<li id="for:value:component:_publisher">' . $publisher_name_html . '</li>';

    if (defined $issn && !defined $issn2) {
        $return_str .= '<li id="for:value:component:_issn">' . html_escape($issn) . '</li>';
    }

    $return_str .= '<li id="for:block:absolute:publisher_policy">';
    $return_str .= '<a href="https://openpolicyfinder.jisc.ac.uk/" target="_new"><img src="/style/images/OPF.png" style="float: right; padding-right: 1em"/></a>';
    $return_str .= 'Journal autocompletion information is derived from the <a href="https://openpolicyfinder.jisc.ac.uk/" target="_new">Open Policy Finder</a> database, an online resource that aggregates and analyses publisher open access policies.';
    $return_str .= '<p>This publication, <a title="Link to the publication information on Open Policy Finder" target="_new" href="https://openpolicyfinder.jisc.ac.uk/id/publication/'
        . $publication_id_url
        . '">'
        . $title_html
        . '</a>, is published by <a target="_new" title="Link to the publisher information on Open Policy Finder" href="https://openpolicyfinder.jisc.ac.uk/id/publisher/'
        . $publisher_id_url
        . '">'
        . $publisher_name_html
        . '</a>.</p>';

    if ($permitted_oa_versions) {
        $return_str .= "The depositor can archive $permitted_oa_versions $versions without additional Open Access fees.";
    }
    else {
        my $possible_with_fee = grep { $_->[6] eq 'with_oa_fee' } @PolicySummary;

        $return_str .= $possible_with_fee
            ? "The depositor cannot archive any version in $repository_html without an additional open access fee to the publisher."
            : "The depositor cannot archive any version in $repository_html.";
    }

    for my $policy (@PolicySummary) {
        next if $policy->[6] eq 'with_oa_fee';

        if (
            $policy->[1] ne ''
            || $policy->[2]
            || $policy->[3]
            || $policy->[4]
            || $policy->[5]
            || $policy->[7]
        ) {
            $return_str .= '<p>';
            $return_str .= "The publisher also defines the following conditions for deposit of $policy->[0] version:<ul>";
            $return_str .= "<li>Required license of deposit: $policy->[7]</li>" if $policy->[7];
            $return_str .= "<li>Prerequisite condition(s):<ul>$policy->[3]</ul></li>" if $policy->[3];
            $return_str .= "<li>Prerequisite funder(s):<ul>$policy->[4]</ul></li>" if $policy->[4];
            $return_str .= "<li>Prerequisite subject(s):<ul>$policy->[5]</ul></li>" if $policy->[5];
            $return_str .= "<li>Embargo period of $policy->[2]</li>" if $policy->[2];
            $return_str .= $policy->[1] if $policy->[1];
            $return_str .= '</ul></p>';
        }
    }

    $return_str .= '</li></ul></li>' . "\n";

    return $return_str;
}
