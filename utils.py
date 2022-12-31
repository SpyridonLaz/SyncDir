import subprocess
from pathlib import Path


class Utils:
    """
    Some dialog methods to make the program interavtive!
    """

    def _zenity_dialog(self, prompt_title="title", zenity_args=""):
        try:
            cmd_output = subprocess.Popen(
                ["zenity", "--title",
                 prompt_title,
                 "--file-selection",
                 zenity_args,
                 ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, )
            cmd_output, err = cmd_output.communicate()

            if cmd_output:
                cmd_output = cmd_output.decode("utf-8").strip("\n")
                return Path(cmd_output)
            else:
                raise AssertionError("You must choose a file")
        except (AssertionError, FileNotFoundError):
            # fallback to manual labor
            return self.fallback_prompt()
        except Exception as e:
            exit(e)

    def fallback_prompt(self):
        # fallback in case OS does not have kdialog/ zenity
        while True:
            user_input = input("Reverting to manual input. Type directory path: ")
            input_as_path = Path(user_input).resolve()
            if input_as_path.exists():
                if input_as_path.is_dir():
                    break
                else:
                    print("Done!")
            elif user_input in ("X", "x"):
                print("Aborted")
                return None
            else:
                user_input = input("Directory does not exist. Create new? [y/n]")
                if user_input in ('y', 'Y'):
                    Path(input_as_path).mkdir(parents=True, exist_ok=True)
                    print("Done!")
                    break
                else:
                    print("Press any key to retry or [X] for exit")
                    continue

        return Path(input_as_path)


    def directory_path(self, title: str = ""):

        # figuring if there are enviromental variables
        # to use for easier file opening
        return self._zenity_dialog(
            prompt_title=f"Choose {title} Directory",
            zenity_args="--directory") if True else self.fallback_prompt()

    def path_to_open(self, ):

        # figuring if there are enviromental variables
        # to use for easier file opening
        return self._zenity_dialog(prompt_title="Open File", ) if True else self.fallback_prompt()

    def path_to_save(self, ):
        return self._zenity_dialog(
            prompt_title="Save File",
            zenity_args="--save")
