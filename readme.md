# No-Shows
## Project overview
Hospital no-shows can lead to inefficient use of resources and increased waiting times.  
This project leverages machine learning to predict the likelihood of inpatient no-shows based on a variety of features like demographics and past appointment history. The model can be found at `5_deployment/back-end`. Be sure to preprocess the features using the preprocessing scripts to get the data ready for inference. 

A dashboard has been created to display the 10 patients, out of the top 20, most likely to miss their appointments. It provides key information such as appointment details and patient phone numbers, making it easier for staff to reach out to those at risk of no-shows. The back-end system ensures that this list is automatically updated daily, forecasting potential no-shows for after three workdays. This enables staff to contact patients in advance and possibly prevent a no-show.

Please note that the dashboard has been tailored for ZGT’s specific setup, so certain queries or design elements may not be transferable to other healthcare settings. While it can serve as a reference, adjustments may be required for use in different infrastructures.

## Usage
*Presumptions: Docker is installed and the user has a Shinyproxy server for the shiny application*
* **Back-end**
1. Create two database tables called `NoShowPreds_curr` and `NoShowPreds_all` according to the following database schema:  

| COLUMN_NAME     | IS_NULLABLE | DATA_TYPE | CHARACTER_MAXIMUM_LENGTH |
|-----------------|-------------|-----------|--------------------------|
| PATIENTNR       | YES         | nvarchar  | 50                       |
| NAAM            | YES         | nvarchar  | 255                      |
| TELEFOON        | YES         | nvarchar  | 255                      |
| STARTDATEPLAN   | YES         | date      | NULL                     |
| STARTTIMEPLAN   | YES         | nvarchar  | 5                        |
| SPECIALISM      | YES         | nvarchar  | 10                       |
| PREDICTIE       | YES         | float     | NULL                     |
| GEBELD          | YES         | nvarchar  | 10                       |
| STATUS          | YES         | nvarchar  | 50                       |
| GEBDAT          | YES         | date      | NULL                     |
| GESLACHT        | YES         | nvarchar  | 10                       |
| GROUP_AB        | YES         | nvarchar  | 20                       |
| LOCATIE         | YES         | nvarchar  | 50                       |`
   

2. In this codebase navigate to `5_deployment/back-end`  
3. Fill in the database credentials found in `config.yaml`
4. Build the docker image using the command: `docker build no_show_back_end .`
5. Run the docker image (on your desired server) using the command: `docker run -d -v ~/NoShows:/app/py no_show_back_end`

* **Front-end**
1. Navigate to `5_deployment/front-end`
2. Fill in the database credentials found in `shiny/config.R`
3. Build the docker image using the command: `docker build no_show_front_end .`
4. Save the docker image using `docker image save -o no_show_front_end.tar no_show_front_end`
5. Transfer the image to your Shinyproxy server
6. Load the docker image on the server where Shinyproxy is running using the command: `docker image load -i no_show_front_end.tar`
7. Configure the shiny application in the `application.yaml` file from shinyproxy

## Contact
For any question please contact Job Maathuis at j.maathuis@zgt.nl or github@zgt.nl