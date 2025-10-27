# Required packages: requests, beautifulsoup4
# Install using: pip install requests beautifulsoup4
# System requirement: wget must be installed
import os
import sys
import re
import math
import signal
import subprocess
import argparse
import datetime
import requests
import textwrap
import xml.etree.ElementTree as ET # For parsing DAT files
from bs4 import BeautifulSoup      # For parsing HTML

#Define constants
#Myrient HTTP-server addresses
MYRIENTHTTPADDR = 'https://myrient.erista.me/files/'
#Catalog URLs, to parse out the catalog in use from DAT
CATALOGURLS = {
    'https://www.no-intro.org': 'No-Intro',
    'http://redump.org/': 'Redump'
    # Add other catalog URLs here if needed
}
#Postfixes in DATs to strip away from the system name
DATPOSTFIXES = [
    ' (Retool)'
    # Add other postfixes if needed
]
#Headers to use in HTTP-requests to mimic a browser (used for Myrient navigation only)
REQHEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
}

# --- Helper Functions ---

# Print output function with timestamp and optional color
def logger(msg, color=None, rewrite=False):
    """Logs a message to the console with a timestamp and optional color."""
    colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'cyan': '\033[96m'}
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if rewrite:
        # ANSI escape code to move cursor up one line and clear the current line
        print('\033[1A', end='\x1b[2K')
    output = f'{timestamp} | {msg}'
    if color and color in colors:
        print(f'{colors[color]}{output}\033[00m') # Apply color and reset
    else:
        print(output)

# Input request function with timestamp and optional color
def inputter(prompt, color=None):
    """Requests user input with a timestamped and optionally colored prompt."""
    colors = {'red': '\033[91m', 'green': '\033[92m', 'yellow': '\033[93m', 'cyan': '\033[96m'}
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_prompt = f'{timestamp} | {prompt}'
    if color and color in colors:
        val = input(f'{colors[color]}{full_prompt}\033[00m') # Apply color and reset
    else:
        val = input(full_prompt)
    return val

# Scale file size to human-readable format (KiB, MiB, etc.)
def scale1024(val_bytes):
    """Converts a size in bytes to a human-readable string (e.g., 1.2 MiB)."""
    prefixes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
    if val_bytes <= 0: 
        power = 0
    else:
        # Determine the appropriate power of 1024
        power = min(int(math.log(val_bytes, 2) / 10), len(prefixes) - 1)
    # Calculate the scaled value
    scaled_value = float(val_bytes) / (2 ** (10 * power))
    unit = prefixes[power]
    return f"{scaled_value:.1f} {unit}"

# Sanitize string for use as a filename/directory name
def sanitize_filename(name):
    """Removes or replaces characters invalid for filenames/directory names."""
    # Remove characters that are generally invalid in filenames across OSes
    sanitized = re.sub(r'[\\/*?:"<>|]', '', name)
    # Replace multiple whitespace characters with a single space and strip leading/trailing whitespace
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()
    # If the name becomes empty or just a dot after sanitization, provide a default
    if not sanitized or sanitized == '.':
        return "default_output_name"
    return sanitized

# Exit handler function for graceful exit on Ctrl+C
def exithandler(signum, frame):
    """Handles Ctrl+C interruption to exit gracefully."""
    logger('Exiting script due to user request (Ctrl+C)!', 'red')
    sys.exit(1)
# Register the exit handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, exithandler)

# --- Argument Parsing ---
# Setup argparse to handle command-line arguments
parser = argparse.ArgumentParser(
    add_help=False, # Disable default help to use a custom group
    formatter_class=argparse.RawTextHelpFormatter, # Allows for better formatting of help text
    description=textwrap.dedent('''\
        \033[92mTool to automatically download ROMs of a DAT-file from Myrient.
        Generate a DAT-file with the tool of your choice to include ROMs that you
        want from a No-Intro/Redump/etc catalog, then use this tool to download
        the matching files from Myrient.\033[00m
    '''))

# Group for required arguments
requiredargs = parser.add_argument_group('\033[91mRequired arguments\033[00m')
requiredargs.add_argument('-i', dest='inp', metavar='nointro.dat', help='Input DAT-file containing wanted ROMs', required=True)

# Group for optional arguments
optionalargs = parser.add_argument_group('\033[96mOptional arguments\033[00m')
optionalargs.add_argument(
    '-o', dest='out', metavar='/data/roms', default=None,
    help='Output path for ROM files to be downloaded.\nIf omitted, creates a directory named after the system/collection\nin the script\'s location.'
)
optionalargs.add_argument('-c', dest='catalog', action='store_true', help='Choose catalog manually, even if automatically found')
optionalargs.add_argument('-s', dest='system', action='store_true', help='Choose system collection manually, even if automatically found')
optionalargs.add_argument('-l', dest='list', action='store_true', help='List only ROMs that are not found in server (if any)')
optionalargs.add_argument(
    '--skip-existing', dest='skipexisting', action='store_true',
    help='Skip download if file already exists in the destination, regardless of size.'
)
optionalargs.add_argument('-h', '--help', dest='help', action='help', help='Show this help message') # Custom help argument
args = parser.parse_args()

# --- Variable Initialization ---
catalog = None                  # e.g., "No-Intro", "Redump"
collection = None               # e.g., "Nintendo - Game Boy Advance", "Sony - PlayStation"
system_name_from_dat = "Unknown System" # System name extracted from DAT header
wantedroms = []                 # List of base ROM names (without extension) from DAT
wantedfiles = []                # List of dicts for files found on server that are in wantedroms
missingroms = []                # List of ROM names from DAT not found on server
availableroms = {}              # Dict of ROMs available in the selected Myrient collection {basename: {details}}
foundcollections = []           # Temp list for auto-detected collections if multiple match
output_dir = None               # Final absolute path for downloads

# --- Argument Validation ---
if not os.path.isfile(args.inp):
    logger(f'Invalid input DAT-file: {args.inp}', 'red')
    sys.exit(1)

# If output path is given, normalize it
if args.out:
    output_dir = os.path.abspath(os.path.normpath(args.out))

# --- DAT File Processing ---
logger('Opening and parsing input DAT-file...', 'green')
try:
    datxml = ET.parse(args.inp)
    datroot = datxml.getroot()
except ET.ParseError as e:
    logger(f'Error parsing DAT file {args.inp}: {e}', 'red')
    sys.exit(1)

# Extract information from DAT header and game entries
for datchild in datroot:
    if datchild.tag == 'header':
        name_element = datchild.find('name')
        url_element = datchild.find('url')
        if name_element is not None and name_element.text:
            system_name_from_dat = name_element.text
            # Clean up system name by removing known postfixes
            for fix in DATPOSTFIXES:
                system_name_from_dat = system_name_from_dat.replace(fix, '')
        if url_element is not None and url_element.text:
            catalogurl_from_dat = url_element.text
            # Try to identify catalog from URL
            if catalogurl_from_dat in CATALOGURLS:
                catalog = CATALOGURLS[catalogurl_from_dat]
                logger(f'Processing {catalog}: {system_name_from_dat}...', 'green')
            else:
                logger(f'Processing {system_name_from_dat} (Catalog URL not recognized: {catalogurl_from_dat})...', 'green')
        else:
             logger(f'Processing {system_name_from_dat} (No URL in DAT header)...', 'green')

    elif datchild.tag == 'game' and 'name' in datchild.attrib:
        # The <game name="..."> attribute is the base name of the game/set.
        # This was corrected in v7 to not use os.path.splitext here.
        basename = datchild.attrib['name']
        if basename not in wantedroms:
            wantedroms.append(basename)

if not wantedroms:
     logger('No games found in the DAT file!', 'red')
     sys.exit(1)

# --- Myrient Interaction: Catalog Selection ---
catalogurl_path = None # Relative path to the catalog on Myrient
logger('Fetching Myrient main directory...', 'cyan')
try:
    resp = requests.get(MYRIENTHTTPADDR, headers=REQHEADERS, timeout=30)
    resp.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', id='list') # Myrient uses a table with id="list"
    if not table or not table.tbody: 
        logger('Could not find expected table structure on Myrient main page.', 'red')
        sys.exit(1)
    maindir_rows = table.tbody.find_all('tr')

    # Attempt to auto-select catalog if identified from DAT
    if catalog: 
        for row in maindir_rows[1:]: # Skip header row of the table
            cell = row.find('td')
            link = cell.find('a') if cell else None 
            if link and 'title' in link.attrs and 'href' in link.attrs: 
                if catalog in link['title']: # Match catalog name in link title
                    catalogurl_path = link['href']
                    logger(f'Automatically selected catalog: {link["title"]}', 'green')
                    break
    
    # If auto-selection failed or manual selection is forced
    if not catalogurl_path or args.catalog: 
        if args.catalog and catalogurl_path: # User forced manual despite auto-find
             logger('Manual catalog selection forced via -c argument.', 'yellow')
        elif not catalog: # Catalog couldn't be determined from DAT
             logger('Catalog could not be determined from DAT file.', 'yellow')
        else: # Catalog was determined but not found on Myrient
             logger(f'Could not automatically find directory for catalog "{catalog}" on Myrient.', 'yellow')

        logger('Please select the catalog from the following list:', 'yellow')
        dirnbr = 1
        catalogtemp = {} # Temporary dict to map selection number to catalog info
        for row in maindir_rows[1:]:
            cell = row.find('td')
            link = cell.find('a') if cell else None
            if link and 'title' in link.attrs and 'href' in link.attrs:
                logger(f'{str(dirnbr).ljust(2)}: {link["title"]}', 'yellow')
                catalogtemp[dirnbr] = {'name': link['title'], 'url': link['href']}
                dirnbr += 1

        if not catalogtemp: 
             logger('No directories found on Myrient main page!', 'red')
             sys.exit(1)

        # Loop until valid user input for catalog selection
        while True:
            sel_str = inputter('Input selected catalog number: ', 'cyan')
            try:
                sel = int(sel_str)
                if 1 <= sel < dirnbr: # Check if selection is in range
                    catalog = catalogtemp[sel]['name']
                    catalogurl_path = catalogtemp[sel]['url']
                    logger(f'Selected catalog: {catalog}', 'green')
                    break
                else:
                    logger('Input number out of range!', 'red')
            except ValueError:
                logger('Invalid input. Please enter a number.', 'red')
            except KeyError: 
                 logger('Internal error: Invalid selection key.', 'red') # Should not happen if range is correct

except requests.exceptions.RequestException as e:
    logger(f'Error fetching Myrient main directory: {e}', 'red')
    sys.exit(1)
except Exception as e: # Catch other potential errors like parsing issues
    logger(f'Error parsing Myrient main directory HTML: {e}', 'red')
    sys.exit(1)


# --- Myrient Interaction: Collection (System) Selection ---
collectionurl_path = None # Relative path to the collection on Myrient
logger(f'Fetching directory for catalog: {catalog}...', 'cyan')
full_catalog_url = f'{MYRIENTHTTPADDR}{catalogurl_path}'
try:
    resp = requests.get(full_catalog_url, headers=REQHEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', id='list')
    if not table or not table.tbody:
        logger(f'Could not find expected table structure on catalog page: {full_catalog_url}', 'red')
        sys.exit(1)
    contentdir_rows = table.tbody.find_all('tr')

    # Attempt to auto-select collection based on system_name_from_dat
    foundcollections = [] # Stores potential matches
    for row in contentdir_rows[1:]:
        cell = row.find('td')
        link = cell.find('a') if cell else None
        if link and 'title' in link.attrs and 'href' in link.attrs:
            # Check if the Myrient collection title starts with the system name from DAT
            if link['title'].startswith(system_name_from_dat):
                 foundcollections.append({'name': link['title'], 'url': link['href']})
    
    # If exactly one match and not forced manual, select it
    if len(foundcollections) == 1 and not args.system:
        collection = foundcollections[0]['name']
        collectionurl_path = foundcollections[0]['url']
        logger(f'Automatically selected collection: {collection}', 'green')

    # If auto-selection failed, multiple matches, or manual selection forced
    if not collectionurl_path or args.system: 
        if args.system and collectionurl_path: 
             logger('Manual collection selection forced via -s argument.', 'yellow')
        elif len(foundcollections) > 1: 
             logger(f'Multiple possible collections found for "{system_name_from_dat}". Please choose:', 'yellow')
        elif len(foundcollections) == 0: 
             logger(f'Could not automatically find a collection matching "{system_name_from_dat}". Please choose:', 'yellow')
        else: # Fallback or if args.system is true with one auto-find
             logger(f'Please select the collection manually:', 'yellow')

        dirnbr = 1
        collectiontemp = {} # For listing all collections if needed

        # Decide which list to present to the user
        if len(foundcollections) > 1 and not args.system: 
            # Present only the auto-detected likely matches
            for i, found in enumerate(foundcollections):
                logger(f'{str(i+1).ljust(2)}: {found["name"]}', 'yellow')
            dirnbr = len(foundcollections) + 1 
        else: 
            # Present all collections from the current catalog directory
             for row in contentdir_rows[1:]:
                 cell = row.find('td')
                 link = cell.find('a') if cell else None
                 if link and 'title' in link.attrs and 'href' in link.attrs:
                    logger(f'{str(dirnbr).ljust(2)}: {link["title"]}', 'yellow')
                    collectiontemp[dirnbr] = {'name': link['title'], 'url': link['href']}
                    dirnbr += 1
             # Ensure there's something to select if this path is taken
             if not collectiontemp and not (len(foundcollections) > 1 and not args.system) : 
                  logger(f'No collection directories found in catalog: {catalog}', 'red')
                  sys.exit(1)
        
        # Loop for user input for collection selection
        while True:
            sel_str = inputter('Input selected collection number: ', 'cyan')
            try:
                sel = int(sel_str)
                if 1 <= sel < dirnbr:
                    if len(foundcollections) > 1 and not args.system:
                        # Selection is from the filtered 'foundcollections' list
                        collection = foundcollections[sel-1]['name']
                        collectionurl_path = foundcollections[sel-1]['url']
                    else:
                        # Selection is from 'collectiontemp' (all items)
                        if sel in collectiontemp: # Check if key exists
                             collection = collectiontemp[sel]['name']
                             collectionurl_path = collectiontemp[sel]['url']
                        else:
                             # This case handles if foundcollections was not used and collectiontemp was not populated yet
                             # (e.g. args.system=True with one auto-find, or no auto-finds at all)
                             if not collectiontemp: # Rebuild collectiontemp if it's empty
                                 rebuild_idx = 1
                                 for row_rebuild in contentdir_rows[1:]:
                                     cell_rebuild = row_rebuild.find('td')
                                     link_rebuild = cell_rebuild.find('a') if cell_rebuild else None
                                     if link_rebuild and 'title' in link_rebuild.attrs and 'href' in link_rebuild.attrs:
                                         collectiontemp[rebuild_idx] = {'name': link_rebuild['title'], 'url': link_rebuild['href']}
                                         rebuild_idx += 1
                             # Try accessing collectiontemp again after potential rebuild
                             if sel in collectiontemp:
                                 collection = collectiontemp[sel]['name']
                                 collectionurl_path = collectiontemp[sel]['url']
                             else: 
                                 logger('Internal error: Selection mapping failed. Please check list numbers.', 'red')
                                 continue # Ask for input again
                    logger(f'Selected collection: {collection}', 'green')
                    break
                else:
                    logger('Input number out of range!', 'red')
            except ValueError:
                logger('Invalid input. Please enter a number.', 'red')
            except (KeyError, IndexError): 
                 logger('Internal error: Invalid selection index or key.', 'red')

except requests.exceptions.RequestException as e:
    logger(f'Error fetching catalog directory {full_catalog_url}: {e}', 'red')
    sys.exit(1)
except Exception as e: 
    logger(f'Error parsing catalog directory HTML: {e}', 'red')
    sys.exit(1)

# --- Determine and Create Output Directory ---
if output_dir is None: # If -o was not provided by user
    if collection: # Collection name must be known to create a sensible directory name
        script_dir = os.path.dirname(os.path.abspath(__file__)) # Get directory where script is located
        sanitized_collection_name = sanitize_filename(collection)
        output_dir = os.path.join(script_dir, sanitized_collection_name)
        logger(f"Output directory not specified (-o). Using automatically generated path: {output_dir}", 'cyan')
    else:
        # This should ideally not be reached if collection selection is successful
        logger("Error: Output directory not specified and collection name could not be determined.", 'red')
        sys.exit(1)

# Create the output directory if it doesn't exist
if not os.path.isdir(output_dir):
    try:
        logger(f'Attempting to create output directory: {output_dir}', 'yellow')
        os.makedirs(output_dir, exist_ok=True) # exist_ok=True prevents error if dir already exists
        logger(f'Output directory created successfully.', 'green')
    except OSError as e:
        logger(f'Error creating output directory: {output_dir} - {e}', 'red')
        sys.exit(1)
logger(f"Using output directory: {output_dir}", "cyan")


# --- Myrient Interaction: Listing Collection Contents ---
logger(f'Fetching contents for collection: {collection}...', 'cyan')
full_collection_url = f'{MYRIENTHTTPADDR}{catalogurl_path}{collectionurl_path}'
try:
    resp = requests.get(full_collection_url, headers=REQHEADERS, timeout=60) # Longer timeout for potentially large dirs
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', id='list')
    if not table or not table.tbody:
        logger(f'Could not find expected table structure on collection page: {full_collection_url}', 'red')
        sys.exit(1)
    collectiondir_rows = table.tbody.find_all('tr')

    # Build dictionary of available ROMs on the server
    # Key: base ROM name (without extension), Value: dict with full filename and URL
    for row in collectiondir_rows[1:]:
        cell = row.find('td')
        link = cell.find('a') if cell else None
        if link and 'title' in link.attrs and 'href' in link.attrs:
            filename_from_myrient = link['title'] # This is the full filename from Myrient, e.g., "Game.Name.v1.ipf"
            # Get base name by removing extension, for matching against DAT game names
            romname_from_myrient, _ = os.path.splitext(filename_from_myrient) 
            file_url = f'{MYRIENTHTTPADDR}{catalogurl_path}{collectionurl_path}{link["href"]}' # Construct full URL
            availableroms[romname_from_myrient] = {'name': romname_from_myrient, 'file': filename_from_myrient, 'url': file_url}

except requests.exceptions.RequestException as e:
    logger(f'Error fetching collection directory {full_collection_url}: {e}', 'red')
    sys.exit(1)
except Exception as e: 
    logger(f'Error parsing collection directory HTML: {e}', 'red')
    sys.exit(1)

if not availableroms:
     logger(f'No files found in the selected collection directory on Myrient: {collection}', 'yellow')


# --- Comparison and Summary ---
# Compare ROMs from DAT (wantedroms) with ROMs available on Myrient (availableroms)
for dat_game_name in wantedroms: # dat_game_name is the full name from DAT's <game name="...">
    if dat_game_name in availableroms: # availableroms keys are also full base names from Myrient
        wantedfiles.append(availableroms[dat_game_name])
    else:
        missingroms.append(dat_game_name)

logger(f'Amount of wanted ROMs in DAT-file   : {len(wantedroms)}', 'green')
logger(f'Amount of found ROMs at server      : {len(wantedfiles)}', 'green')
if missingroms:
    logger(f'Amount of missing ROMs at server    : {len(missingroms)}', 'yellow')


# --- Download Files (or List Missing) ---
if args.list: # If -l is specified, only list missing files
    logger("Listing mode enabled (-l). No files will be downloaded.", "cyan")
else: # Proceed with download
    logger(f"Starting download of {len(wantedfiles)} files...", "green")
    dlcounter = 0
    total_files_to_download = len(wantedfiles)
    num_digits_for_counter = len(str(total_files_to_download)) if total_files_to_download > 0 else 1 

    for wantedfile_details in wantedfiles:
        dlcounter += 1
        # Construct full local path for the file
        localpath = os.path.join(output_dir, wantedfile_details["file"])
        # Formatted counter string for logging (e.g., [001/123])
        counter_str = str(dlcounter).zfill(num_digits_for_counter)
        log_prefix = f"[{counter_str}/{total_files_to_download}]"

        # Check if file exists and --skip-existing is enabled
        if args.skipexisting and os.path.isfile(localpath):
            logger(f"{log_prefix} Skipping (--skip-existing): {wantedfile_details['name']} already exists.", 'green', rewrite=(dlcounter > 1))
            continue  # Skip to next file

        # --- Download using wget ---
        logger(f"{log_prefix} Downloading: {wantedfile_details['name']}", 'cyan', rewrite=(dlcounter > 1))

        # Build wget command with appropriate flags
        wget_cmd = [
            'wget',
            '-c',                      # Continue/resume partial downloads
            '-O', localpath,           # Output file path
            '--timeout=600',           # 10 minutes timeout
            '--tries=3',               # Retry up to 3 times on errors
            '--progress=bar:force',    # Force progress bar display
            '--show-progress',         # Show progress statistics
            '-q',                      # Quiet mode (only show progress)
            wantedfile_details['url']  # URL to download
        ]

        try:
            # Execute wget and capture result
            result = subprocess.run(wget_cmd, check=False, capture_output=False)

            # Check wget exit code
            if result.returncode == 0:
                logger(f"{log_prefix} Downloaded: {wantedfile_details['name']}", 'green', rewrite=False)
            elif result.returncode == 1:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Generic wget error", 'red', rewrite=False)
            elif result.returncode == 3:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: File I/O error", 'red', rewrite=False)
            elif result.returncode == 4:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Network failure", 'red', rewrite=False)
            elif result.returncode == 5:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: SSL verification failure", 'red', rewrite=False)
            elif result.returncode == 6:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Authentication failure", 'red', rewrite=False)
            elif result.returncode == 7:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Protocol error", 'red', rewrite=False)
            elif result.returncode == 8:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Server error", 'red', rewrite=False)
            else:
                logger(f"{log_prefix} Error downloading {wantedfile_details['name']}: Wget exit code {result.returncode}", 'red', rewrite=False)

        except FileNotFoundError:
            logger(f"{log_prefix} ERROR: wget is not installed or not in PATH. Please install wget.", 'red', rewrite=False)
            logger("On Debian/Ubuntu: sudo apt-get install wget", 'red')
            logger("On Fedora/RHEL: sudo dnf install wget", 'red')
            logger("On macOS: brew install wget", 'red')
            sys.exit(1)
        except Exception as e_generic:
            logger(f"{log_prefix} Unexpected error running wget for {wantedfile_details['name']}: {e_generic}", 'red', rewrite=False)

    # Final message after all downloads attempted
    if total_files_to_download > 0 and not args.list:
         logger('Downloading complete!', 'green', rewrite=False) 
    elif not args.list: # No files were in wantedfiles list
         logger('No files needed downloading.', 'green')


# --- Output Missing ROMs ---
if missingroms:
    logger(f'\n--- Missing ROMs ---', 'yellow') # Add a newline for better separation
    logger(f'Following {len(missingroms)} ROMs in DAT were not found in Myrient collection "{collection}":', 'red')
    for missing_rom_name in missingroms: # missing_rom_name is the full name from DAT
        logger(missing_rom_name, 'yellow')
else:
    logger('\nAll ROMs in DAT found in the selected Myrient collection!', 'green')

