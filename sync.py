import hashlib
import os
import platform
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas
import pandas as pd


class Sync:
    counter = 0

    def __init__(
            self,
            source_path: str,
            replica_path: str,
            log_path: str,
            name: str = "Default_{c}".format(c=counter),
            interval: int = 600,
            context=None,

    ):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)


        self.context = context
        self.counter += 1
        self.name = name
        self.source = Path(source_path),
        self.replica = Path(replica_path),
        self.log = Path(log_path)
        self.set_source_dir(source_path)
        self.set_replica_dir(replica_path)
        self.set_log_dir(log_path)
        self.interval = interval

    def record_log(self, record):
        print(self.log)
        with  Path(self.log, "log_operations").open('a') as f:
            print(record)
            f.write(record)

    def set_source_dir(self, path):
        #  set source directory
        path = Path(path)
        _dir = self._set_directory(path=path)
        self.source = _dir, self._data_frame(path=path) if _dir else self.source
        # returns bool on success/fail
        return path == self.source[1]

    def set_replica_dir(self, path):
        #  set replica(destination) directory
        path = Path(path)

        _dir = self._set_directory(path=path)
        self.replica = _dir, self._data_frame(path=path) if _dir else self.replica
        # returns bool on success/fail
        return path == self.replica[1]

    def set_log_dir(self, path):
        path = Path(path)
        # directory to store log files
        _dir = self._set_directory(path=path)
        self.log = _dir if _dir else self.log
        # returns bool on success/fail
        return path == self.log

    def _set_directory(self, path: Path, ):
        if not Path(path).exists():
            # create directory if it does not exist
            Path(path).mkdir(parents=True, exist_ok=True)
        elif not path.is_dir():
            raise FileExistsError()
        # return if path is created else raise exception
        return path if path.exists() else False

    def set_interval(self, interval: int):
        """
        :param interval:  (seconds) Only positive integers are accepted
        """
        self.interval = interval if isinstance(interval, int) and interval > 0 else self.interval

    def _data_frame(self, path: Path, pattern: str = "*"):

        df = pd.DataFrame(
            [[item.relative_to(Path(item.parts[0])),
              item,
              "dir" if item.is_dir() else "file",
              self.get_md5(item) if item.is_file() else item.relative_to(Path(item.parts[0])),
              self._get_ctime(item),
              item.stat().st_mtime,
              item.stat().st_atime,

              ] for item in path.rglob(pattern)],

            columns=["path",
                     "name",
                     "type",
                     "md5",
                     "created",
                     "modified",
                     "accessed",
                     ])

        return df

    def get_md5(self, file):
        # read file
        with open(file, "rb") as f:
            hash = hashlib.md5(f.read()).hexdigest()
        # now return it's md5 value in hex
        return hash

    def _get_ctime(self, file):
        #  get creation timestamp from metadata
        meta = Path.stat(file)
        _os = platform.system()
        # check platform  to avoid issues
        if _os == 'Windows':
            return os.path.getctime(meta)
        elif _os == "Linux":
            return meta.st_ctime if meta.st_ctime else meta.st_mtime
        else:
            return meta.st_birthtime

    def _get_mtime(self, file):
        #  get modified timestamp from metadata
        return file.stat().st_mtime

    def _get_atime(self, file):
        #  get accessed timestamp from metadata
        return file.stat().st_atime

    def _get_new_files(self, source_df, dest_df):
        # returns DataFrame with all new files.
        return source_df.merge(dest_df, on='md5', how='left', indicator=True)




    def _get_dest_minus_source(self, source_df, dest_df,custom=None  ):
        # remove stuff from destination
        _minus=  dest_df.merge(custom, on='md5', indicator=False, how='left') .query('_merge == "left_only"').drop('_merge', 1)


        return _minus

    def _get_intersection(self, source_df, dest_df):
        """
        Returns intersectional operations,
         from which, the right (destination dir) has unique "md5" rows.
         We do this proccess to avoid unecessary copy between source and destination,
         and instead, copy from within the same storage or ,even better, simply rename
         another file that might have the exact same data.

        """
        intersection: pandas.DataFrame = source_df.merge(dest_df, on='md5', indicator=True, sort=False,                                                         how='inner').drop_duplicates(subset=['path_x'], )
        return intersection

    def _remove_out_of_sync(self, df=None):
        # we remove files from replica that do not exist in source dir.
        # based on their md5 columns.
        print("removing out of sync")
        to_deletion =  self._get_dest_minus_source(self.source[1] , self.replica[1],custom=df)
        print("TO DELETION",to_deletion)
        for i, op in to_deletion.iterrows():
            try:
                if op.name_x.is_dir() :
                    os.rmdir(op.name_x)
                    self.record_log(f"File deleted: {op.name_x.name}  Time {time.time()}")
                else:
                    os.remove(op.name_x)
                    self.record_log(f"File deleted: {op.name_x.name}  Time {time.time()}")
                print("DELETED:      " ,op.name_x)
            except Exception as e:
                print (e)
                print("WAS NOT DELETED:      ", op.name_x,op.name_y)

        return self._copy_full


    def merge_new_files(self):

        n_workers = 4
        with ProcessPoolExecutor(n_workers) as exe:
            for i, op in self.source[1].iterrows():
                _new_name = Path(op.name.parent, (f"{op.name.name}"))

                _ = exe.submit(self._copy_full,op.name,  _new_name)
        return _

    def _copy_full(self, source, dest):
        shutil.copy2(source, dest)  # OVERWRITES
        self.record_log(f"File copied : {source.name_y}  Time {time.time()}")
        return
    def _copy_intersection(self, ):
        _inter = self._get_intersection(self.source[1], self.replica[1])

        n_workers = 4
        with ProcessPoolExecutor(n_workers) as exe:
            for i, op in _inter.iterrows():

                _new_name = Path(op.name_y.parent, (f"{op.path_x.name}.new"))

                try:
                    if op.name_y.is_dir():
                        _new_name.mkdir(parents=True, exist_ok=True)
                        self.record_log(f"File created : {_new_name}  Time {time.time()}")
                        _ = exe.submit(   shutil.copytree(op.name_y, _new_name))
                        self.record_log(f"Metadata copied : {op.name_y}  Time {time.time()}")
                    else:
                        _new_name.parent.mkdir(parents=True, exist_ok=True)

                        _ = exe.submit(                        shutil.copyfile(op.name_y, _new_name))
                        self.record_log(f"File copied : {op.name_y}  Time {time.time()}")
                    shutil.copystat(op.name_x, _new_name)
                except FileExistsError as e:
                    print("Exception: ",e)

        return self._remove_out_of_sync(_inter.loc[_inter['type_x'] == "dir"]    )


    def sync(self):
        #pre-operation we handle
        # existing files and folders
        self._copy_intersection()

