from colorama import Fore
from colorama import Style



def prep_text(txt,with_line_breaker=True):
    if with_line_breaker:
        return f'\n{Fore.GREEN}{txt}{Style.RESET_ALL}\n'
    else:
        return f'{Fore.GREEN}{txt}{Style.RESET_ALL}'

def prep_title(txt):
    return f'\n{Fore.BLUE}{txt}{Style.RESET_ALL}\n'