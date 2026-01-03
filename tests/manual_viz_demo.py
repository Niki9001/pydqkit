from pathlib import Path
import pandas as pd

from pydq.viz import profile_to_html


def build_demo_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["AB123456", "CD000001", "EF999999", None],
            "age": [29, 35, None, 42],
            "score": [88.5, 91.2, 76.0, 91.2],
            "date": ["2025-12-01", "2025-12-02", "2025-12-02", None],
            "city": ["Halifax", "Dartmouth", "Halifax", "Bedford"],
            "flag": [True, False, True, True],
        }
    )


def main() -> None:
    df = build_demo_df()

    out_dir = Path(__file__).resolve().parent / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "manual_demo_profile.html"

    # output file path
    profile_to_html(
        df,
        out_html=str(out_path),
        dataset_name="manual_demo",
    )

    print(f"Saved: {out_path}")
    print("Open it in your browser to view the report.")


if __name__ == "__main__":
    main()
