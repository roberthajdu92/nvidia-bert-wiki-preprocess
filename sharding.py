from collections import defaultdict
from itertools import islice

import os
import multiprocessing
import statistics

import argparse

if not os.path.exists("formatted_text"):
    os.makedirs("formatted_text")

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input_file",
    default=None,
    type=str,
    required=True,
    help="Whole formatted wikicorpus file name."
)

parser.add_argument(
    "--num_shards",
    default=None,
    type=int,
    required=True,
    help="Number of shards to make."
)


class Sharding:
    def __init__(self, input_files, output_name_prefix, n_training_shards, fraction_test_set):
        assert len(
            input_files) > 0, 'The input file list must contain at least one file.'
        assert n_training_shards > 0, 'There must be at least one output shard.'

        self.n_training_shards = n_training_shards
        self.fraction_test_set = fraction_test_set

        self.input_files = input_files

        self.output_name_prefix = output_name_prefix
        self.output_training_identifier = '_training'
        self.output_test_identifier = '_test'
        self.output_file_extension = '.txt'

        self.articles = {}    # key: integer identifier, value: list of articles
        self.sentences = {}    # key: integer identifier, value: list of sentences
        # key: filename, value: list of articles to go into file
        self.output_training_files = {}
        self.output_test_files = {}  # key: filename, value: list of articles to go into file

        self.init_output_files()

    # Remember, the input files contain one article per line (the whitespace check is to skip extraneous blank lines)

    def load_articles(self):
        print('Start: Loading Articles')

        global_article_count = 0
        for input_file in self.input_files:
            print('input file:', input_file)
            with open(input_file, mode='r', newline='\n') as f:
                pline = []
                for i, line in enumerate(f):
                    if not line.strip() and pline:
                        self.articles[global_article_count] = " ".join(pline)
                        self.sentences[global_article_count] = list(pline)
                        global_article_count += 1
                        pline.clear()
                    else:
                        pline.append(line.strip())

        print('End: Loading Articles: There are', len(self.articles), 'articles.')

    def init_output_files(self):
        print('Start: Init Output Files')
        assert len(self.output_training_files) is 0, 'Internal storage self.output_files already contains data. This function is intended to be used by the constructor only.'
        assert len(self.output_test_files) is 0, 'Internal storage self.output_files already contains data. This function is intended to be used by the constructor only.'

        for i in range(self.n_training_shards):
            name = self.output_name_prefix + self.output_training_identifier + \
                '_' + str(i) + self.output_file_extension
            self.output_training_files[name] = []

        print('End: Init Output Files')

    def get_sentences_per_shard(self, shard):
        result = 0
        for article_id in shard:
            result += len(self.sentences[article_id])
        return result

    def distribute_articles_over_shards(self):
        print('Start: Distribute Articles Over Shards')
        assert len(self.articles) >= self.n_training_shards, 'There are fewer articles than shards. Please add more data or reduce the number of shards requested.'

        # Create dictionary with - key: sentence count per article, value: article id number
        sentence_counts = defaultdict(lambda: [])

        max_sentences = 0
        total_sentences = 0

        for article_id in self.sentences:
            current_length = len(self.sentences[article_id])
            sentence_counts[current_length].append(article_id)
            max_sentences = max(max_sentences, current_length)
            total_sentences += current_length

        n_sentences_assigned_to_training = int(
            (1 - self.fraction_test_set) * total_sentences)
        nominal_sentences_per_training_shard = n_sentences_assigned_to_training // self.n_training_shards

        consumed_article_set = set({})
        unused_article_set = set(self.articles.keys())

        # Make first pass and add one article worth of lines per file
        for file in self.output_training_files:
            current_article_id = sentence_counts[max_sentences][-1]
            sentence_counts[max_sentences].pop(-1)
            self.output_training_files[file].append(current_article_id)
            consumed_article_set.add(current_article_id)
            unused_article_set.remove(current_article_id)

            # Maintain the max sentence count
            while len(sentence_counts[max_sentences]) == 0 and max_sentences > 0:
                max_sentences -= 1

            if len(self.sentences[current_article_id]) > nominal_sentences_per_training_shard:
                nominal_sentences_per_training_shard = len(
                    self.sentences[current_article_id])
                print(
                    'Warning: A single article contains more than the nominal number of sentences per training shard.')

            # Maintain the max sentence count
            while len(sentence_counts[max_sentences]) == 0 and max_sentences > 0:
                max_sentences -= 1

        training_counts = []

        for shard in self.output_training_files:
            training_counts.append(self.get_sentences_per_shard(
                self.output_training_files[shard]))

        training_median = statistics.median(training_counts)

        # Make subsequent passes over files to find articles to add without going over limit
        history_remaining = []
        n_history_remaining = 4

        while len(consumed_article_set) < len(self.articles):
            for fidx, file in enumerate(self.output_training_files):
                nominal_next_article_size = min(
                    nominal_sentences_per_training_shard - training_counts[fidx], max_sentences)

                # Maintain the max sentence count
                while len(sentence_counts[max_sentences]) == 0 and max_sentences > 0:
                    max_sentences -= 1

                while len(sentence_counts[nominal_next_article_size]) == 0 and nominal_next_article_size > 0:
                    nominal_next_article_size -= 1

                if nominal_next_article_size not in sentence_counts or nominal_next_article_size is 0 or training_counts[fidx] > training_median:
                    continue    # skip adding to this file, will come back later if no file can accept unused articles

                current_article_id = sentence_counts[nominal_next_article_size][-1]
                sentence_counts[nominal_next_article_size].pop(-1)

                self.output_training_files[file].append(current_article_id)
                consumed_article_set.add(current_article_id)
                unused_article_set.remove(current_article_id)

            # If unable to place articles a few times, bump up nominal sizes by fraction until articles get placed
            if len(history_remaining) == n_history_remaining:
                history_remaining.pop(0)
            history_remaining.append(len(unused_article_set))

            history_same = True
            for i in range(1, len(history_remaining)):
                history_same = history_same and (
                    history_remaining[i-1] == history_remaining[i])

            if history_same:
                nominal_sentences_per_training_shard += 1
                # nominal_sentences_per_test_shard += 1

            training_counts = []

            for shard in self.output_training_files:
                training_counts.append(self.get_sentences_per_shard(
                    self.output_training_files[shard]))

            training_median = statistics.median(training_counts)

            #if len(unused_article_set) < 29:
             #   break

            print('Distributing data over shards:', len(
                unused_article_set), 'articles remaining.')

        if len(unused_article_set) != 0:
            print('Warning: Some articles did not make it into output files.')

        for shard in self.output_training_files:
            print('Training shard:', self.get_sentences_per_shard(
                self.output_training_files[shard]))

        print('End: Distribute Articles Over Shards')

    def write_shards_to_disk(self):
        print('Start: Write Shards to Disk')
        for shard in self.output_training_files:
            self.write_single_shard(shard, self.output_training_files[shard])

        print('End: Write Shards to Disk')

    def write_single_shard(self, shard_name, shard):
        with open(shard_name, mode='w', newline='\n') as f:
            for article_id in shard:
                for line in self.sentences[article_id]:
                    f.write(str(line) + '\n')

                f.write('\n')  # Line break between articles


args = parser.parse_args()

sharding = Sharding([args.input_file], "formatted_text/formatted_text",
                    args.num_shards, 0)

sharding.load_articles()
sharding.distribute_articles_over_shards()
sharding.write_shards_to_disk()
