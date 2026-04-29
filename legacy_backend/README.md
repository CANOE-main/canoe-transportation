# Legacy Transportation Backend Reference

This directory preserves the pre-v2.0 CANOE transportation backend. It is kept as validation evidence for the v2.0 refactor, not as the preferred active workflow.

## Canadian Open Energy Model (CANOE) - Transportation Sector

**Framework for the transportation energy system database of the Canadian Open Energy Model.**

This repository contains the compiled input data, processing scripts, and analysis tools for the transportation sector of the CANOE project. It maps the transportation energy system, including fuel supply, vehicle technologies, and charging profiles.

For documentation on transport, visit: [Here ](https://canoe-main.github.io/canoe-transportation/)

## Project Structure

This repository is organized into several key directories:

- **`./` (Root)**
  - Contains the original schema (`canoe_schema.sql`) and alternative scenario database files (e.g., Reference scenario).
- **`transportation/`**
  - Contains annotated transport sector databases (Excel).
  - Includes the **compiler** (`compile_transport.py`) that converts `.xlsx` tables into the `.sqlite` format required by the vanilla model.
- **`db_processing/`**
  - Scripts and notebooks used to process the compiled database from Excel spreadsheets into usable formats for the [Temoa framework](https://temoaproject.org/).
- **`model_constraints/`**
  - Excel spreadsheets with explicit (user-defined) constraints applied to the vanilla CANOE-transportation model.
- **`charging_profiles/`**
  - Input parameters used to simulate LDV charging profiles with the RAMP-mobility framework.
- **`results_analysis/`**
  - Jupyter notebooks used to analyze and visualize CANOE-transportation scenario results.
- **`fuel_supply/`**
  - Fuel supply input data used in the spreadsheet databases.

## Getting Started

### Prerequisites

To run the compiler and analysis notebooks, you will need **Python 3.x** and the following packages:

- `pandas`
- `numpy`
- `jupyter` (for notebooks)

You can install the core dependencies via pip:

```bash
pip install pandas numpy jupyter
```

### Compiling the Database

To compile the transportation Excel spreadsheets into a Temoa-compatible SQLite database:

1.  Navigate to the project root.
2.  Run the compilation script:

```bash
python transportation/compile_transport.py
```

*Note: Ensure your environment relies on the paths defining in the script, or update them as necessary.*

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to help improve this project.

## License

This project is licensed under the terms defined in the [LICENSE](LICENSE) file.

## Support

For support and questions, please refer to [SUPPORT.md](SUPPORT.md).
