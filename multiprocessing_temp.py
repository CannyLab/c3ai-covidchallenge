import multiprocessing as mp
import os
import pandas as pd
import re
from tqdm import tqdm

def parse_filename(data):
    '''parse_filename 
    
       function used with create_kinetics_dataset_txt_file pool multiprocessing 


    Args:
        data ([type]): [description]
    '''
    i, f = data
    try:
        if ('.mp4' in f):
            # remove the .mp4 extension name as well as the _01 or _00 extension that was added when the .mp4 was split during preprocessing
            source_f = f.replace(list(re.finditer('_[0-9]+\.mp4', f))[0].group(), '') # b/c during creation of the kinetics dataset the file was split
            # search the pandas df containing the loaded .csv file with maps from the youtube id to the label (sorted by alphabetical order)
            label = df[df['youtube_id'] == source_f]['int_label'].values[0]
            # if the above line doens't through an indexerror then the label exists and we should add f and the label to the lines we want to save
            manager_list.append(f"{path}/{f} {label}")
        else:
            print(f, " not being added to filelist")
    except IndexError as e: #meaning that the df returned no values for the source_f
        print(i, "\n", e)
        print(f)
        print('continuing')
        
    
def create_kinetics_dataset_txt_file(dataset_root_path: str, dataset_file_path : str, csv_path: str, num_pools : int  = 1):
    '''create_kinetics_dataset_txt_file [summary]

    Args:
        dataset_root_path (str): the path to the root directory containing the mp4 files
        dataset_file_path (str): the path where the list of filenames and space seperated label will be saved
        csv_path (str): the location to the kinetics csv dataset associated with 
    '''
    df = pd.read_csv(csv_path)
    labels = [label for label in df.iloc[:, 0].value_counts().index.values]
    inverse_label_map = dict([(label, i) for (i, label) in enumerate(sorted(labels))])
    df['int_label'] = df['label'].apply(lambda label: inverse_label_map[label])
    
    def init_worker(df, path, manager_list):
        df = df
        path = path
        manager_list = manager_list
    
    
    for path, subdir, files in os.walk(dataset_root_path):
        manager = mp.Manager()
        global_lines = manager.list()
        with mp.Pool(num_pools, initializer=init_worker, initargs=(df, path)) as pool:
            for data in tqdm(pool.imap_unordered(parse_filename, enumerate(files)), total=len(files)):
                continue

    with open(dataset_file_path, 'w') as f:
        f.write("\n".join(global_lines))
    print("\tsaved at ", dataset_file_path)
    
