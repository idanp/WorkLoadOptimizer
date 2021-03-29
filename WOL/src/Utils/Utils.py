from colorama import Fore
from colorama import Style
import multiprocessing


def prep_text(txt,with_line_breaker=True):
    if with_line_breaker:
        return f'\n{Fore.GREEN}{txt}{Style.RESET_ALL}\n'
    else:
        return f'{Fore.GREEN}{txt}{Style.RESET_ALL}'

def prep_title(txt):
    return f'\n{Fore.BLUE}{txt}{Style.RESET_ALL}\n'

task_queue = None
def get_syncro_queue():
    global task_queue
    if not task_queue:
        task_queue = multiprocessing.JoinableQueue()
    return task_queue
