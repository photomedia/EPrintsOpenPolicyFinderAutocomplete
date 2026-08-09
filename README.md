# EPrintsOpenPolicyFinderAutocomplete

extract journal info and IR publisher policies from JISC Open Policy Finder 


## Compatibility

Compatible with Open Policy Finder API (post July 2026 api change)

https://openpolicyfinder.jisc.ac.uk/help/developers/how-to-access-the-api-platform

## Usage

### Quick Mode

Quick mode refreshes a single Open Policy Finder publication record without rebuilding the full autocomplete file: run it as ./get_journal_titles_policies.pl --quick-id <publication_id> or simply ./get_journal_titles_policies.pl <publication_id>, for example --quick-id 30984 for Laboratory Medicine, --quick-id 17599 for PLoS ONE, or --quick-id 18195 for Partnership. It fetches the live API record, prints the ===== API JSON ITEM ===== and ===== GENERATED HTML ===== trace blocks to STDERR, regenerates that record’s autocomplete HTML, updates journal_modified_cache.json, and patches romeo_journals.autocomplete. Use it after changing policy logic, fee handling, or location filtering, or whenever a cached record needs to be force-regenerated even though the OPF modified timestamp has not changed.

### Full Run

A full run is started by running the script without a quick-mode argument, for example ./get_journal_titles_policies.pl; it fetches the live list of Open Policy Finder publication IDs, compares each record’s live modified timestamp against journal_modified_cache.json, skips unchanged cached records, refreshes changed or missing records through the API, regenerates their autocomplete HTML, writes the rebuilt output to the construct file, then renames it into romeo_journals.autocomplete and saves the updated cache. Use a full run when you want to rebuild or synchronize the entire autocomplete dataset, especially after many OPF records have changed; however, if you changed the script’s processing logic, remember that unchanged cached records may still be skipped, so use quick mode or clear affected cache entries when you need to force regeneration.

### Intermediate Cache Files

The script uses three main output/cache files: romeo_journals.construct is the temporary/intermediate build file used during a full run, romeo_journals.autocomplete is the final autocomplete file that users/applications consume, and journal_modified_cache.json is the cache that stores each OPF publication ID’s last modified timestamp and generated HTML so unchanged records can be skipped on later runs. During writes, the script also creates temporary .tmp files, such as journal_modified_cache.json.tmp and romeo_journals.autocomplete.tmp, then renames them into place after a successful write to avoid leaving partially written final files.


## Acknowledgements

This script was developed by Tomasz Neugebauer at Concordia University (@photomedia) https://github.com/photomedia

The original Sherpa Romeo API version was developed with assistance of (@gobfrey) https://github.com/gobfrey
