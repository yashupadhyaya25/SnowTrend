from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template/yahoo_finance_dag_template.jinja2')

for filename in os.listdir(f"{file_dir}/inputs") :
    print(filename)
    if filename.endswith('.yml') :
        with open(f"{file_dir}/inputs/{filename}","r") as input_file:
            inputs = yaml.safe_load(input_file)
            print(inputs)
            with open(f"/root/airflow/dags/get_price_{inputs['dag_id'].lower()}.py","w") as f:
                f.write(template.render(inputs))