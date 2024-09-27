# datasus-datamodel Changelog

This file provides a full account of all changes to `datasus-datamodel`.
Changes are listed under the release in which they first appear. Subsequent releases include changes from previous releases.
"Breaking changes" listed under a version may require action from end users or external maintainers when upgrading to that version.

## datasus-datamodel 1.0.0 - September 27, 2024

### Features

- **Sample Data Creation**: Added functionality to generate sample data based on IBGE and Datasus datasets, enabling deeper exploration and analysis of public health data. ([#12](https://github.com/VctrEman/datasus-datamodel/issues/12))
- **Optimized Data Download**: Introduced optimized data download using **Multi-threading** and **Process Pool**, significantly improving the efficiency and speed of downloading large datasets from Datasus. ([#15](https://github.com/VctrEman/datasus-datamodel/issues/15))

### Fixes

- Fixed a timeout issue when downloading large datasets from Datasus, which caused intermittent failures. ([#18](https://github.com/VctrEman/datasus-datamodel/issues/18))

### Under the Hood

- Refined the multi-threading logic to ensure thread safety and efficiency during data processing. ([#20](https://github.com/VctrEman/datasus-datamodel/issues/20))
- Enhanced memory management in data pipelines to handle large datasets with reduced overhead. ([#21](https://github.com/VctrEman/datasus-datamodel/issues/21))

### Dependencies

- **PySpark**: For distributed data processing and ETL operations.
- **Pandas**: To handle dataframes and structured data manipulation.
- **Requests**: For making HTTP requests to download data from public APIs (Datasus, IBGE).
- **Threading** and **Multiprocessing**: To manage concurrent tasks and optimize download performance.

### Contributors

- [@VctrEman](https://github.com/VctrEman)
- [@nycolasdiaas](https://github.com/nycolasdiaas)