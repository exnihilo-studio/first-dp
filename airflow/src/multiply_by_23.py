import os

# import the result of the previous task as an environment variable
data_point = os.environ["DATA_POINT"]

# multiply the data point by 23 and package the result into a json
multiplied_data_point = str(23 * int(data_point))
return_json = {"return_value": f"{multiplied_data_point}"}

# write to the file checked by Airflow for XComs
f = open("./airflow/xcom/return.json", "w")
f.write(f"{return_json}")
f.close()
