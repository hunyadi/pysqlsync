"Script to update the `README.md` file in the project root."

import os.path
import re
import textwrap


def update_readme() -> None:
    samples: dict[str, str] = {}

    with open(os.path.join(os.path.dirname(__file__), "example.py"), "r") as f:
        text = f.read()

    for m in re.finditer(
        r"^\s*# BLOCK: (?P<label>.+?)\n(?P<code>.+?)\n\s*# END$",
        text,
        re.MULTILINE | re.DOTALL,
    ):
        label = m.group("label")
        code = textwrap.dedent(m.group("code"))
        samples[label] = code

    with open(os.path.join(os.path.dirname(__file__), "..", "README.md"), "r") as f:
        text = f.read()

    def code_block(samples: dict[str, str], label: str, code: str) -> str:
        return f"<!-- {label} -->\n```python\n{samples[label]}\n```"

    text = re.sub(
        r"^<!--\s*(?P<label>.+?)\s*-->\n```python\n(?P<code>.+?)\n```$",
        lambda m: code_block(samples, m.group("label"), m.group("code")),
        text,
        0,
        re.MULTILINE | re.DOTALL,
    )

    with open(os.path.join(os.path.dirname(__file__), "..", "README.md"), "w") as f:
        f.write(text)


if __name__ == "__main__":
    update_readme()
