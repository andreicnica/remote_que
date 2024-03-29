import logging


class RemoteQueLogging(logging.Logger):
    def __init__(
            self,
            name: str,
            level: int,
            filename: str =None,
            filemode: str ="a",
            stream=None,
            format=None,
            dateformat=None,
            style="%",
    ):
        super().__init__(name, level)
        if filename is not None:
            handler = logging.FileHandler(filename, filemode)
        else:
            handler = logging.StreamHandler(stream)
        self._formatter = logging.Formatter(format, dateformat, style)
        handler.setFormatter(self._formatter)
        super().addHandler(handler)

    def add_filehandler(self, log_filename):
        filehandler = logging.FileHandler(log_filename)
        filehandler.setFormatter(self._formatter)
        self.addHandler(filehandler)


logger = RemoteQueLogging(
    name="remote-que", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
