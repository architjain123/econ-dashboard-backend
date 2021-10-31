import json, gzip, shutil
from os import walk


class FileManager:

    @staticmethod
    def get_files_to_parse(path="parsed_files.json"):
        filesToParse = []
        with open(path, 'r') as f:
            jsonFile = json.loads(f.read())
            parsedFiles = set(jsonFile["files"])
            filesInDir = next(walk("files"), (None, None, []))[2]

            for file in filesInDir:
                if file.startswith("CA-CORE_POI-PATTERNS") and file.endswith(".zip") and \
                        FileManager.rename(file) not in parsedFiles:
                    filesToParse.append(file)

        return filesToParse

    @staticmethod
    def rename(file):
        return file[:28]

    @staticmethod
    def parse_year_month(filename):
        filesplit = filename.split("-")[3]
        year = filesplit[:4]
        month = filesplit[-2:]
        return int(year), int(month)

    @staticmethod
    def unzip_gz_file(gz_file):
        try:
            with gzip.open(gz_file, 'rb') as f_in:
                with open(gz_file.replace('.gz', ""), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except:
            return

    @staticmethod
    def add_to_parsed_files(file):
        with open('parsed_files.json', 'r+') as f:
            data = json.load(f)
            data['files'].append(FileManager.rename(file))
            f.seek(0)  # reset file position to the beginning.
            json.dump(data, f, indent=4)
            f.truncate()  # remove remaining part