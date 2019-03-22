import datetime
import logging
import re
import subprocess
import shlex

from humanfriendly.tables import format_pretty_table

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
        self._xtrabackup_history_log_headers = ['command', 'xtrabackup_function', 'process_id', 'result',
                                                'start time', 'end time', 'duration', 'exit code']
        self._xtrabackup_history_log = []

    @property
    def xtrabackup_history_log(self):
        result = format_pretty_table(self._xtrabackup_history_log, self._xtrabackup_history_log_headers)
        return result

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
        cmd_start = datetime.datetime.now()
        with subprocess.Popen(subprocess_args, stdout=PIPE, stderr=STDOUT) as process:
            for line in process.stdout:
                logger.debug("[{}:{}] {}".format(subprocess_args[0], process.pid, line.decode("utf-8").strip("\n")))
#            for line in process.stderr:
#                logger.error("[{}:{}] {}".format(subprocess_args[0], process.pid, line.decode("utf-8").strip("\n")))
        logger.info("SUBPROCESS {} COMPLETED with exit code: {}".format(subprocess_args[0], process.returncode))
        cmd_end = datetime.datetime.now()
        result_str = "Success" if process.returncode == 0 else "Failure"

        self.summarize_process(subprocess_args, result_str, cmd_start, cmd_end, process.returncode, process.pid)
        # return True or False.
        if process.returncode == 0:
            return True
        else:
            # todo: optionally raise error instead of return false
            # todo: cnt'd or, if any subprocess fails, can we stop in a recoverable state?
            logger.error("A subprocess: {} returned a non-zero exit code.".format(subprocess_args[0]))
            logger.error("Look above for ERROR level logs from process_runner.".format(subprocess_args[0]))
            raise ChildProcessError

            #return False

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

    @staticmethod
    def represent_duration(start_time, end_time):
        # https://gist.github.com/thatalextaylor/7408395
        duration_delta = end_time - start_time
        seconds = int(duration_delta.seconds)
        days, seconds = divmod(seconds, 86400)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        if days > 0:
            return '%dd%dh%dm%ds' % (days, hours, minutes, seconds)
        elif hours > 0:
            return '%dh%dm%ds' % (hours, minutes, seconds)
        elif minutes > 0:
            return '%dm%ds' % (minutes, seconds)
        else:
            return '%ds' % (seconds,)

    def summarize_process(self, args, result_str, cmd_start, cmd_end, return_code, process_id):

        cmd_root = args[0].split("/")[-1:][0]
        xtrabackup_function = None
        if cmd_root == "xtrabackup":
            if "--backup" in args:
                xtrabackup_function = "backup"
            elif "--prepare" in args and "--apply-log-only" not in args:
                xtrabackup_function = "prepare"
            elif "--prepare" in args and "--apply-log-only" in args:
                xtrabackup_function = "prepare/apply-log-only"
        if not xtrabackup_function:
            for arg in args:
                if re.search(r'(--decrypt)=?[\w]*', arg):
                    xtrabackup_function = "decrypt"
                elif re.search(r'(--decompress)=?[\w]*', arg):
                    xtrabackup_function = "decompress"

        if cmd_root == "pigz":
            # this will be just the "pigz --version" check, don't bother logging...
            return True

        self._xtrabackup_history_log_headers = ['command', 'xtrabackup_fn', 'duration', 'result',
                                                'start time', 'end time', 'exit code', 'process_id']
        self._xtrabackup_history_log.append([cmd_root,
                                             xtrabackup_function,
                                             self.represent_duration(cmd_start, cmd_end),
                                             result_str,
                                             cmd_start.strftime('%Y-%m-%d %H:%M:%S'),
                                             cmd_end.strftime('%Y-%m-%d %H:%M:%S'),
                                             return_code,
                                             process_id])
        return True


ProcessRunner = ProcessHandler()
