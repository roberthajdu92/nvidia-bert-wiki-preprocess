import subprocess
import os
import argparse

parser = argparse.ArgumentParser()

parser.add_argument(
    '--n_processes',
    type=int,
    help='Specify the max number of processes to allow at one time',
    default=4
)

parser.add_argument(
    '--random_seed',
    type=int,
    help='Specify the base seed to use for any random number generation',
    default=12345
)

parser.add_argument(
    '--dupe_factor',
    type=int,
    help='Specify the duplication factor',
    default=5
)

parser.add_argument(
    '--masked_lm_prob',
    type=float,
    help='Specify the probability for masked lm',
    default=0.15
)

parser.add_argument(
    '--max_seq_length',
    type=int,
    help='Specify the maximum sequence length',
    default=512
)

parser.add_argument(
    '--max_predictions_per_seq',
    type=int,
    help='Specify the maximum number of masked words per sequence',
    default=20
)

parser.add_argument(
    '--do_lower_case',
    type=int,
    help='Specify whether it is cased (0) or uncased (1) (any number greater than 0 will be treated as uncased)',
    default=1
)

parser.add_argument(
    '--vocab_file',
    type=str,
    required=True,
    help='Specify absolute path to vocab file to use)'
)

args = parser.parse_args()

hdf5_tfrecord_folder_prefix = "hdf5_lower_case_" + str(args.do_lower_case) + "_seq_len_" + str(args.max_seq_length) \
    + "_max_pred_" + str(args.max_predictions_per_seq) + "_masked_lm_prob_" + str(args.masked_lm_prob) \
    + "_random_seed_" + \
    str(args.random_seed) + "_dupe_factor_" + str(args.dupe_factor)

last_process = None

if not os.path.exists(hdf5_tfrecord_folder_prefix + "/wikicorpus"):
    os.makedirs(hdf5_tfrecord_folder_prefix + "/wikicorpus")


def create_record_worker(shard_id, output_format='hdf5'):
    bert_preprocessing_command = 'python3 create_pretraining_data.py'
    bert_preprocessing_command += ' --input_file=' + \
        'formatted_text/formatted_wiki_training_' + str(shard_id) + '.txt'
    bert_preprocessing_command += ' --output_file=' + hdf5_tfrecord_folder_prefix + \
        '/wikicorpus/wiki_training_' + str(shard_id) + '.' + output_format
    bert_preprocessing_command += ' --vocab_file=' + str(args.vocab_file)
    bert_preprocessing_command += ' --do_lower_case' if args.do_lower_case else ''
    bert_preprocessing_command += ' --max_seq_length=' + \
        str(args.max_seq_length)
    bert_preprocessing_command += ' --max_predictions_per_seq=' + \
        str(args.max_predictions_per_seq)
    bert_preprocessing_command += ' --masked_lm_prob=' + \
        str(args.masked_lm_prob)
    bert_preprocessing_command += ' --random_seed=' + str(args.random_seed)
    bert_preprocessing_command += ' --dupe_factor=' + str(args.dupe_factor)
    bert_preprocessing_process = subprocess.Popen(
        bert_preprocessing_command, shell=True)

    last_process = bert_preprocessing_process

    # This could be better optimized (fine if all take equal time)
    if shard_id % args.n_processes == 0 and shard_id > 0:
        bert_preprocessing_process.wait()
    return last_process


files = os.listdir("formatted_text")

for i in range(len(files)):
    last_process = create_record_worker(i)


last_process.wait()
