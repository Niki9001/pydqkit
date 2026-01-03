# pydqkit

**pydqkit** is a lightweight, developer-first Python toolkit for **data quality profiling and interactive HTML visualization**.

It helps data engineers and analysts quickly understand the structure, completeness, and patterns of tabular datasets, without requiring any external platforms or services.

---

## Features

- Column-level data profiling
  - Null and non-null statistics
  - Distinct and duplicate counts
  - Length analysis (minimum, maximum, average)
- Pattern discovery for string, datetime, and boolean columns
- Type inference (numeric, string, boolean, datetime)
- Interactive, self-contained HTML profiling reports
- Designed for exploratory analysis and debugging workflows

---

## Installation

```bash
pip install pydqkit
```

---

## Quick Start

```python
import pandas as pd
from pydqkit.profiling import profile_dataframe
from pydqkit.viz import profile_to_html

df = pd.DataFrame({
    "id": ["AB123456", "CD654321", None, "EF000001"],
    "age": [25, 30, None, 40],
    "score": [88.5, 92.0, 79.5, 85.0],
    "date": ["2025-01-01", "2025-01-02", None, "2025-01-04"],
    "flag": [True, True, False, True],
})

profile = profile_dataframe(df, dataset_name="demo")

html = profile_to_html(profile)
with open("profile_report.html", "w", encoding="utf-8") as f:
    f.write(html)
```

Open `profile_report.html` in your browser to explore the interactive profiling report.

---

## What the Profiling Report Shows

For each column, the report includes:

### Completeness
- Proportion of non-null and null values (visual bar and percentages)

### Value Statistics
- Distinct count and duplicate count

### Type Information
- Inferred profile type and suggested logical type

### Length Metrics
- Minimum length (reported as 0 if missing values exist)
- Maximum and average length

### Pattern Summary
- Common structural patterns inferred from values

### Top Values
- Most frequent values with counts and percentages

The report is designed to be readable at a glance while still exposing enough detail for debugging and validation.

---

## Design Philosophy

pydqkit is intentionally:

### Developer-first
Optimized for notebooks, scripts, and local inspection.

### Platform-independent
No cloud services, no accounts, no metadata dependencies.

### Explainable
Metrics and visual elements are designed to be interpretable rather than opaque.

The project follows conventions commonly used in enterprise data quality tools, but is implemented as a standalone Python library.

---

## License

This project is released under the **MIT License**.  
See the [LICENSE](LICENSE) file for details.

The author reserves the right to relicense future versions.

---

## Disclaimer

This project is an independent open-source toolkit and is **not affiliated with, endorsed by, or associated with Informatica or any other commercial data quality platform**.

---

## Contributing

Contributions are welcome.

By submitting a pull request, you agree that your contributions will be licensed under the same license as this project.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## Roadmap (Tentative)

- Rule-based data quality checks
- Column-level and cell-level validation
- Support for multiple rule definitions:
  - Regular expressions
  - SQL-based rules
  - Plain-language rule specifications
- Data engineering quality checks (schema, type, and pipeline sanity)
- Machine learning data sanity checks (distribution drift, outliers)
- Column comparison across datasets
- Cell level data quality check
- Export profiling reports to multiple formats (HTML, PDF, Excel)
- Configurable thresholds and validation summaries

---

## Author

**Niki Zheng**
