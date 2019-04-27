# Overview

This project serves as an extensible framework for processing and cleaning historical FX data from Dukascopy's data feed.

# Usage

The most direct usage is through the `load_fx_data.py` script. There are several command line options:

`python load_fx_data.py --pair=EURUSD --start_date=2019-01-01 --end_date=2019-02-01 --opath=/data/EURUSD/raw --processes=4 --pipeline=tabular`

- `pair`: Currency pair for historical data.
- `start_date`: Starting point for data processing.
- `end_date`: (Optional) Ending point for data processing not inclusive of the final date. Default behavior sets end date to current date.
- `opath`: Output directory to write batches of files.
- `processes`: (Optional) Number of processes to run. If too many are run, then the user will start to receive 503 responses from the server. 4-8 processes are normally ideal.
- `pipeline`: (Optional) Specify any custom pipelines added to the `pipelines/` directory. Default option is `tabular`. 

# Layout

- `network`: Request handling and parsing.
- `pipelines`: Fully connected data processing pipelines.
- `proceesors`: Data processing and cleaning.
- `tests`: Unit tests.
- `utils`: Utility and tooling functions.

Modifying data pipelines is generally as simple as adding a new class that inherits from the relevant base class.

# To-Do

- Improve testing suite.
- Implement options for different pipelines.
- Implement resampling tools for lower resolution views.
- Devlelop custom exceptions and improve exception handling.
