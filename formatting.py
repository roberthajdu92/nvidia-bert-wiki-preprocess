import glob
import argparse

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input_folder",
    default=None,
    type=str,
    required=True,
    help="Input folder for extracted wiki files."
)

parser.add_argument(
    "--output_file",
    default=None,
    type=str,
    required=True,
    help="Output file name."
)


class WikicorpusTextFormatting:
    def __init__(self, wiki_path, output_filename, recursive=False):
        self.wiki_path = wiki_path
        self.recursive = recursive
        self.output_filename = output_filename

    # This puts one article per line

    def merge(self):
        with open(self.output_filename, mode='w', newline='\n') as ofile:
            for dirname in glob.glob(self.wiki_path + '/*/', recursive=False):
                for filename in glob.glob(dirname + 'wiki_*', recursive=self.recursive):
                    print(filename)
                    article_lines = []
                    article_open = False

                    with open(filename, mode='r', newline='\n', encoding="utf8", errors='ignore') as file:
                        for line in file:
                            if '<doc id=' in line:
                                article_open = True
                            elif '</doc>' in line:
                                article_open = False
                                for oline in article_lines[1:]:
                                    if oline != '\n':
                                        ofile.write(str(oline).rstrip() + " ")
                                ofile.write("\n\n")
                                article_lines = []
                            else:
                                if article_open:
                                    article_lines.append(line)


args = parser.parse_args()

wiki_formatter = WikicorpusTextFormatting(
    args.input_folder, args.output_file, recursive=True)
wiki_formatter.merge()
