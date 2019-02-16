import datetime
import logging
import re
import subprocess
import shlex

from subprocess import PIPE, STDOUT

from general_conf.generalops import GeneralClass
from general_conf import path_config


logger = logging.getLogger(__name__)


class ProcessHandler(GeneralClass):
    """
    Class to run a command with real-time logging for process

    centralizes logic for subprocess calls, and is available to all other classes (Prepare, Backup, etc)
    """
    def __init__(self, config=path_config.config_path_file):
        self.conf = config
        GeneralClass.__init__(self, self.conf)


    def run_command(self, command):
        """
        executes a prepared command, enables real-time console & log output.

        This function should eventually be used for all subprocess calls.

        :param command: bash command to be executed
        :type command: str
        :return: True if success, False if failure
        :rtype: bool
        """
        # filter out password from argument list, print command to execute

        filtered_command = re.sub("--password='?\w+'?", "--password='*'", command)
        logger.info("SUBPROCESS STARTING: {}".format(filtered_command))
        subprocess_args = self.command_to_args(command_str=command)
        # start the command subprocess

        with subprocess.Popen(subprocess_args, stdout=PIPE, stderr=STDOUT) as process:
            for line in process.stdout:
                logger.debug("[{}:{}] {}".format(subprocess_args[0], process.pid, line.decode("utf-8").strip("\n")))
        logger.info("SUBPROCESS {} COMPLETED with exit code: {}".format(subprocess_args[0], process.returncode))

        # return True or False.
        if process.returncode == 0:
            return True
        else:
            # todo: optionally raise error instead of return false
            # todo: cnt'd or, if any subprocess fails, can we stop in a recoverable state?
            return False

    @staticmethod
    def command_to_args(command_str):
        """
        convert a string bash command to an arguments list, to use with subprocess

        Most autoxtrabackup code creates a string command, e.g. "xtrabackup --prepare --target-dir..."
        If we run a string command with subprocess.Popen, we require shell=True.
        shell=True has security considerations (below), and we run autoxtrabackup with privileges (!).
        https://docs.python.org/3/library/subprocess.html#security-considerations
        So, convert to an args list and call Popen without shell=True.

        :param command_str: string command to execute as a subprocess
        :type command_str: str
        :return: list of args to pass to subprocess.Popen.
        :rtype: list
        """
        if isinstance(command_str, list):
            # already a list
            args = command_str
        elif isinstance(command_str, str):
            args = shlex.split(command_str)
        else:
            raise TypeError
        logger.debug("subprocess args are: {}".format(args))
        return args


ProcessRunner = ProcessHandler()
