from functools import partial
import requests
import re
from tqdm import tqdm
import json
import asyncio
import httpx
import csv
import os
import aiometer


# Dev variables
QUERY_THEGRAPH = False
QUERY_INFURA = False


# Global Variables
INFURA_PROJECT_ID = ''
INFURA_PROJECT_SECRET = ''
INFURA_IPFS_API_URL = 'https://ipfs.infura.io:5001/api/v0/cat'
INFURA_MAX_REQUESTS_PER_SEC = 10
THEGRAPH_API_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/zer0-os/zns'
DOMAIN_GROUPS = [
    'wilder.wheels.genesis',
    'wilder.kicks.airwild.season0',
    'wilder.kicks.airwild.season1',
    'wilder.kicks.airwild.season2',
    'wilder.moto.genesis',
    'wilder.beasts.wolf',
    'wilder.craft.genesis',
    'wilder.cribs.wiami.southbeach.qube' ]


def get_total_domains() -> int:
    """
    Queries the total number of zNS domains using official zer0-tech GraphQL API.

    Returns:
        total_domains (int): The total number of zNS domains
    """
    query = '{ domains(first: 1, where: {indexId_not: null}, orderBy: indexId, orderDirection: desc) {indexId} }'
    request = requests.post(
        THEGRAPH_API_ENDPOINT,
        json={'query': query}
    )
    if request.status_code == 200:
        request = request.json()
        total_domains = int(request['data']['domains'][0]['indexId'])
    return total_domains


def get_all_domains(maxId: int) -> list:
    """
    Queries the data of all zNS domains (token id, domain id, domain name, IPFS path where metadata is stored). 
    Official zer0-tech GraphQL API is used to retrieve all the data. It limits 1000 objects to be retrieved per query.
    
    Parameters:
        maxId (int): The total number of domains to query

    Returns: 
        result (list): A list of dictionnaries including the data about all zNS domains
    """
    results = []
    lastId=0 # Used to query API by chunks of 1000
    with tqdm(desc='Quering domains', total=maxId) as pbar:
        while True:
            query = '''
            {
                domains(first: 1000, where: { indexId_gt: ''' f'{lastId}' ''' }, orderBy: indexId, orderDirection: asc) {
                    id
                    indexId
                    name
                    metadata
                }
            }
            '''
            request = requests.post(THEGRAPH_API_ENDPOINT, json={'query': query})
            if request.status_code == 200:
                # If we get a response, update the cursor, format the response in JSON and calculate how many domains we retrieved
                lastId = lastId + 1000
                request = request.json()
                entries_retrieved = len(request['data']['domains'])
                if len(request['data']['domains']) <= 0: # If don't obtain any domain, we stop the loop and return what we got so far
                    break
                else:
                    results.extend(request['data']['domains'])
                    pbar.update(entries_retrieved) # Increase the progress bar by the number of domain which were returned to us
            else:
                raise Exception('Query failed. return code is {}. {}'.format(request.status_code, query))
    return results


def check_for_duplicated_entries(data: list) -> list:
    """
    Checks if there is any duplicated entries in a list of dictionnaries, by comparing the 'name' value between all entries.

    Parameters:
        data (list): A list of dictionnaries which have the key 'name'

    Returns:
        duplicated (list): A list of entries which have been seen at least 2 times in the data
    """
    names = [z['name'] for z in data]
    seen = set()
    duplicated = []
    for name in names:
        if name not in seen:
            seen.add(name)
        else:
            duplicated.append(name)
    return duplicated


def put_json_data_in_file(data: list, filename: str) -> None:
    """
    Outputs data into a file.

    Parameters:
        data (list): A list of dictionnaries which can be serialized into JSON (not apostrophees sensitive)
        filename (str): The name of the file where the data will be written
    """
    with open(filename, 'w') as f:
        f.write(json.dumps(data))


def format_ipfs_hash_in_dicts(data: list) -> tuple:
    """
    Replaces IPFS paths in data dictionnaries 'metadata' field by the IPFS hash

    Parameters:
        data (list): List of dictionnaries where each dictionnary represents a zNS domain. Each entry needs to have a 'metadata' field with an IPFS path
    
    Returns:
        data (list): Same list as the 'data' parameter but with all 'metadata' fields containing the IPFS hash pointing to the metadata of the zNS domain
    """
    # Replace 'ipfs://<hash>' by '<hash>' in 'metadata' field
    not_match = []
    pattern = re.compile(re.escape('ipfs://'))
    for entry in data:
        hash = re.sub(pattern, '', entry['metadata'])
        if hash == entry['metadata']:
            not_match.append(entry)
        else:
            entry['metadata'] = hash
    return (data, not_match)


async def get_metadata(session: httpx.AsyncClient, data: dict, pbar: tqdm = None) -> list:
    """
    Queries Infura API to retrieve metadata of a given zNS domain. It reads the IPFS hash in the 'metadata' field of the 'data' parameter.
    Then it requests IPFS through Infura API to obtain the corresponding metadata, and puts the result into the 'metadata' field of the 'data' parameter.
    At the beginning, the 'metadata' field contains the IPFS hash ; at the end it contains a dictionnary of the true metadata.

    Parameters:
        session (httpx.AsyncClient): An instance of the AsyncClient object to do asynchronous requests
        data (dict): A dictionnary representing the zNS domain. It has to contain a 'metadata' field with an IPFS hash ('Qm...')
        pbar (tqdm): (optionnal) An instance to a tqdm progress bar which can be used to increment it
    Returns:
        data (dict): The same dictionnary (parsed in JSON) which was passed as a parameter, but with the domain's metadata instead of the IPFS 
            hash in the 'metadata' field.
    """
    params = { 'arg': data['metadata'] }
    response = await session.post(
        INFURA_IPFS_API_URL, 
        params=params, 
        auth=(INFURA_PROJECT_ID, INFURA_PROJECT_SECRET)
    )
    response = response.json()
    data['metadata'] = response
    if pbar is not None:
        pbar.update(1)
    return data


async def get_all_metadata(data: list, maxId: int) -> list:
    """
    Retrieve metadata of all zNS domains represented by dictionnaries in a list.

    Parameters:
        data (dict): A list of dictionnaries, where each dictionnary represents a zNS domain
        maxId (int): The total number of zNS, in order to query the metadata for all of them

    Returns:
        results (list): A list of all zNS domains with the corresponding metadata
    """
    session = httpx.AsyncClient()
    results = []
    with tqdm(total=maxId, desc='Retrieving metadata') as pbar:
        results = await aiometer.run_all(
            [partial(get_metadata, session, entry, pbar) for entry in data], 
            max_per_second=50,
            max_at_once=100
        )
    return results


def separate_industries_into_files(data: list, domain_groups: list) -> None:
    """
    Create a file for all industries which are given by 'domain_groups' and put the corresponding data in it.
    For each domain in 'domain_groups', it takes all of its direct childs (and not subnodes) and puts them into the file.

    Parameters:
        data (list): List of dictionnaries of all zNS domains metadata
        domain_groups (list): List of zNS domain names, used to sort the data into different file
    """
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, r'industries')
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)
    for domain_group in domain_groups:
        current_group = []
        for entry in data:
            pattern = re.compile(re.escape(domain_group) + r'\.[^.]+$')
            if re.search(pattern, entry['name']):
                current_group.append({'tokenId':entry['id'], 'domain':entry['name'], 'metadata':entry['metadata']})
        put_json_data_in_file(current_group, os.path.join(final_directory, domain_group))


def flatten(input_dict, separator='.', prefix='') -> dict:
    """
    Flatten a dictionnary by unnesting subdictionnaries and sublists. Keys are transformed in a string of subkeys separated by a dot.
    Example : input = {'a': 'b', 'c': [{'d': 'e'}, {'f', 'g'}]} Output = {'a': 'b', 'c.0.d': 'e', 'c.1.f': 'g'}

    Parameters:
        input_dict (dict): A dictionnary to be flatten
        separator (str): The separator for creating new keys
        prefix (str): A string to put before each subkey

    Output:
        output_dict (dict): The dictionnary flattenned
    """
    output_dict = {}
    for key, value in input_dict.items():
        if isinstance(value, dict) and value:
            deeper = flatten(value, separator, prefix+key+separator)
            output_dict.update({key2: val2 for key2, val2 in deeper.items()})
        elif isinstance(value, list) and value:
            for index, sublist in enumerate(value, start=1):
                if isinstance(sublist, dict) and sublist:
                    deeper = flatten(sublist, separator, prefix+key+separator+str(index)+separator)
                    output_dict.update({key2: val2 for key2, val2 in deeper.items()})
                else:
                    output_dict[prefix+key+separator+str(index)] = value
        else:
            output_dict[prefix+key] = value
    return output_dict


def put_json_to_csv_in_file(data, filename) -> None:
    """
    Put a list of dictionnary into a file. All dictionnaries must have the same keys.

    Parameters:
        data (list): A list of dictionnary to put in the file, where each dictionnary represents a line and each dict key a CSV column.
        filename (str): The name of the file to put the data into
    """
    with open(filename, 'w') as f:
        csv_writer = csv.writer(f)
        count = 0
        for entry in data:
            if count == 0:
                header = entry.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(entry.values())


def main():

    FILE_RAW_OUTPUT = 'output.txt'
    FILE_DOMAINS_WITH_METADATA = 'metadata_output.txt'
    FILE_DOMAINS_WITHOUT_METADATA = 'no_metadata_output.txt'

    data = []
    current_dir = os.path.dirname(os.path.realpath(__file__))
    industries_dir = os.path.join(current_dir, r'industries')
    #flatten_dir = os.path.join(current_dir, r'industries_flatten')
    csv_dir = os.path.join(current_dir, r'industries_csv')

    # Get total domains to retrieve
    total_domains = get_total_domains()

    # Query all domains through TheGraph API
    if QUERY_THEGRAPH == True:
        data = get_all_domains(total_domains)
        put_json_data_in_file(data, FILE_RAW_OUTPUT)
    else:
        with open(FILE_RAW_OUTPUT, 'r') as f:
            data = json.load(f)
            print(f"All domains retrieved in file '{f.name}'.")

    # Check if there is any duplicated entries
    duplicated_entries = check_for_duplicated_entries(data)
    if len(duplicated_entries) != 0:
        raise Exception(f'Duplicated entries were found: {duplicated_entries}')

    # Replace IPFS paths by IPFS hashes and store data without paths in a file
    data, not_match = format_ipfs_hash_in_dicts(data)
    put_json_data_in_file(not_match, FILE_DOMAINS_WITHOUT_METADATA)

    # Retrieve metadata from IPFS hashes through Infura
    if QUERY_INFURA == True:
        data = asyncio.run(get_all_metadata(data, total_domains))
        put_json_data_in_file(data, FILE_DOMAINS_WITH_METADATA)
    else:
        with open(FILE_DOMAINS_WITH_METADATA, 'r') as f:
            data = json.load(f)
            print(f"All metadata retrieved in file '{f.name}'.")

    # Output items into different files, each one corresponding to an industry
    domain_groups_to_output_in_file = DOMAIN_GROUPS
    separate_industries_into_files(data, domain_groups_to_output_in_file)

    # Process all industry files
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    for domain in DOMAIN_GROUPS:
        results = []
        with open(os.path.join(industries_dir, domain), 'r') as f:
            data = json.load(f)
            # Transform attributes
            for i in data:
                attributes = i['metadata']['attributes']
                attributes = sorted(attributes, key=lambda d: d['trait_type'])
                attributes = [{z['trait_type']:z['value']} for z in attributes]
                i['metadata']['attributes'] = attributes
                results.append(i)

            # Flatten all industries and put the results in files
            results = [flatten(i) for i in results]

            # Sort the results by domain name
            results = sorted(results, key=lambda d: d['domain'])

            # Output in flatten dir
            #put_json_data_in_file(results, os.path.join(flatten_dir, domain))

            # Output in CSV format
            put_json_to_csv_in_file(results, os.path.join(csv_dir, domain))


    # Compare wheels
    #wheels_compare = []
    #wheels_sheet = []

    #with open(os.path.join(flatten_dir, 'wilder.wheels.genesis')) as f:
    #    wheels_compare = json.load(f)

    #with open('wheels_metadata_compare.txt', 'r') as f:
    #    wheels_sheet = f.read().split('\n')

    #print(wheels_sheet)
    #for wheel in wheels_compare:
    #    if str(int(wheel['tokenId'], 16)) in wheels_sheet:
    #        pass
    #    else:
    #        print(str(int(wheel['tokenId'], 16)))
    #        pass

    
if __name__ == '__main__':
    main()
