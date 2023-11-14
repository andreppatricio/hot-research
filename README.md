# Hot Research Data Pipeline

Welcome to the Hot Research Data Pipeline project! This data engineering project extracts, processes, and provides visualizations for information about published scientific papers from two reputable APIs, CORE and arXiv. The entire pipeline is orchestrated and automated using Apache Airflow on a weekly basis.

---
![Project Diagram](hot-research-diagram.svg)
---

## Keypoints:
### ETL Process:

- **Data Extraction**: Utilizes APIs from CORE and arXiv to fetch the latest data on scientific papers.

- **Data Processing**: Weekly automated process, orchestrated by Apache Airflow, includes filtering for English papers, removing stopwords and punctuation, and extracting n-grams from the text.

### Database:

- **Structured Storage**: Data is stored in a Postgres database, providing a structured and efficient storage solution.

### Visualization:

- **Jupyter Notebooks**: Utilizes Jupyter notebooks for visualization purposes, offering both standard and interactive options.

- **Interactive Visualizations**: Enables live visualization of the entire database with dynamic plot parameter adjustments.

- **N-gram Analysis**: Emphasizes the analysis of n-grams to identify popular keywords in scientific papers, providing insights into trending topics.

- **PySpark Integration**: For expedited data retrieval, PySpark is used to read data from the database during the visualization process.

### Containerization:

- **Docker Containers**: The project is containerized using Docker, with separate containers for Apache Airflow, the Postgres database, and the Jupyter server + PySpark.

### Continuous Integration:

- **CI Process**: A streamlined continuous integration process is implemented through GitHub Actions.

- **Lint Checks**: Ruff is used to enforce code style and best practices.

- **Unit Testing**: Pytest is employed for unit testing to validate the functionality of the code.

---

## Getting Started:

To run the data pipeline and explore the visualizations, follow these steps:

1. Clone the repository.
2. Navigate to the project directory.
3. Set environment variables in the `.env` file.
4. Run `docker-compose up` to start the containers.
5. Access the Apache Airflow Web UI at `localhost:8080`.
6. Access the Jupyter notebooks for visualizations and analysis at `localhost:8888`.

## Project Structure:

- **`/dags`**: Contains Apache Airflow DAGs for the data extraction, processing, and loading tasks.
- **`/plugins`**: Defines necessary functionalities for the DAGs, such as API access, database access, text processing, and other tasks.
- **`/sql`**: SQL files with relevant queries to interact with the database.
- **`/visualization`**: Jupyter notebooks for visualization and analysis.
- **`/docker-compose.yml`**: Configuration file for Docker containers.
- **`/.github/workflows`**: GitHub Actions workflow for continuous integration.

## Acknowledgments:

We appreciate the contributions of the CORE and arXiv APIs, enabling access to valuable scientific paper data.

Feel free to explore the project!

Happy coding! 🚀