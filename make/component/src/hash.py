


import argparse




def get_argumentParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset-path', type=str,required=True,  help='the path of dataset')
    parser.add_argument('--hash-buckets-size', type=int,required=True,   help='the size of buckets')
    parser.add_argument('--out-hash-path', type=str,required=True,
                        help='the path of preprocessed dataset will be saved')
    parser.add_argument('--entilty-list', type=str,required=True,   help='the entiltys you want to preprocess')
    parser.add_argument('--out-path-file', type=str,required=True,
                        help='hdfs path where the hash dataset has been saved')

    args = parser.parse_args()

    return args



import sys
def get_input():
    return sys.argv
