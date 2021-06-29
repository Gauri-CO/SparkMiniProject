
## Table of Contents
* [General Info](#general-info)
* [Description](#description)
* [Technologies](#technologies)
* [Setup](#setup)
* [Local Execution](#local-execution)
* [Hadoop Execution](#hadoop-execution)



## General Info
This project is PySpark Mini Project that uses Data Frame to get the number of accidents per make and year of the car

## Description
In this project, we need to utilize data from an automobile tracking platform that tracks the history of important incidents after the initial sale of a new vehicle. Such incidents include subsequent private sales, repairs, and accident reports. The platform provides a good reference for second-hand buyers to understand the vehicles they are interested in.

A PySpark program needs to be developed to get the number of accident records per make and year of the car.

## Technologies
* PySpark 3
* Jupyter NoteBook



## Data.csv loaded into DataFrame
![DataFile](https://user-images.githubusercontent.com/75573079/123869826-82f0a080-d8ff-11eb-8b7d-8bdc7a8e18e4.png)



## Create two Dataframes (Accident records and Initial Records)
![Datasets_For_Join](https://user-images.githubusercontent.com/75573079/123869837-871cbe00-d8ff-11eb-9684-b77c3bb777d0.png)



## Inner Join two dataframe to create Final Dataframe, group by Make and Year
![final_output](https://user-images.githubusercontent.com/75573079/123869851-8be17200-d8ff-11eb-94b6-c2140a89b3b2.png)


