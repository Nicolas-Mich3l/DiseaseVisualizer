# OMOPDiseaseVisualizer

A Flask + React app to visualize OMOP-standard data (CMS 2008-2010 Data Entrepreneurs’ Synthetic Public Use File)

Usage guide:

  1. Clone github repository and run docker-compose up --build OR pull container image from [Dockerhub](https://hub.docker.com/layers/ndam1101/omop-visualizer/latest/images/sha256:a4f3b298d53cbfd82cf273cffb1c4ba90d1b838588ee7e2e4c3e695afb9ef116) and build through GUI / CLI
  2. Once the container is built, navigate to localhost:8000 to access it.
  3. Login using any credentials, login flow is a mockup
  4. Select disease type and measurement from dropdown menus and click build cohorts.
  5. Graph functionality (export, zoom, reset zoom) for graphs can be found on the right of them.

Initial build times are slower due to pulling the node image, downloading dependencies and the datasets.

OMOP tables used: condition_occurrence, measurement, person.

Of note: the value_as_number field of the measurement file detailed in the assignment document was empty(in 1k, 100k and 2.3M). In order to keep the spirit of the assignment as I understood it, I have generated data for a Lipid test panel and inserted it into the table. The script used to do so can be found under backend/data_gen.py

Cohort selection logic:

  1. User selects disease in web UI (React)
  2. React application makes API call to Flask (Python) backend requesting cohort stats
  3. Flask API calls dbWrapper (python wrapper class for DuckDB) to execute SQL queries
  4. dbWrapper performs two queries: Select distinct tuples of (person_id, gender_concept_id, year_of_birth) from person table where condition ID matches (or does not match) disease selected
  5. dbWrapper returns the dataframes to FlaskAPI which JSON-ifies the data and forwards it to the frontend
  6. Frontend renders visualizations from the provided JSON using React-Plotly

Measurement logic:

  1. Build cohorts as detailed above
  2. Grab list of person_id from cohort, run another duckDB query which matches rows containing (person_id,measurement_concept_id), building a table of (person_id, value_as_number)
