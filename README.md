# Hot Research Data Pipeline

Welcome to the Hot Research Data Pipeline project! This data engineering project extracts, processes, and provides visualizations for information about published scientific papers from two reputable APIs, CORE and arXiv. The entire pipeline is orchestrated and automated on a weekly basis using Apache Airflow.

---
![Project Diagram](images/hot-research-diagram.svg)
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

## Getting Started and Visualizing the Data:

To run the data pipeline, extract valuable insights, and explore visualizations, follow these steps:

1. **Clone the Repository:**
   - Use `git clone` to clone the repository to your local machine.

2. **Navigate to the Project Directory:**
   - Open a terminal and move to the project directory using `cd`.

3. **Set Environment Variables:**
   - Use the `.env` file in the project root to set the environment variables.

4. **Run the Data Pipeline:**
   - Execute `docker-compose up` in the terminal to start the containers and run the data pipeline.

5. **Access Apache Airflow Web UI:**
   - Open your web browser and go to `localhost:8080` to access the Apache Airflow Web UI.
   - Start the relevant DAGs `core_api_tf`, `arxiv_api_tf`, and `keywords_dag_tf` to extract, process, and load the information.

6. **Explore Visualizations:**

   - **Static Plots:**
     - Open the Jupyter notebook `/visualization/visualizations.ipynb` to view the static plots.

   - **Interactive Plots:**
     - Once the DAGs have completed their tasks, access the Jupyter server running locally at `localhost:8888`.
     - Utilize the Jupyter notebook `/visualization/interactive_visualizations.ipynb` to explore interactive plots.
     - Adjust parameters within the notebook to dynamically select and display specific data.

By following these steps, you can run the entire project locally, extract meaningful insights from scientific paper data, and explore both pre-computed and interactive visualizations.

## Interactive Visualization Example



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
