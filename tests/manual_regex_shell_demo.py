import pandas as pd

from pydqkit.regex_shell import interactive_shell


def main() -> None:
    df = pd.DataFrame(
        {
            "id": ["AB123456", "ZZ000001", "A123", None, "ab123456", "AA999999"],
            "status": ["ACTIVE", "INACTIVE", "BAD", "ACTIVE", None, "inactive"],
            "amount": [10, 20.5, "30", None, "xx", 999],
        }
    )

    interactive_shell(df)


if __name__ == "__main__":
    main()
