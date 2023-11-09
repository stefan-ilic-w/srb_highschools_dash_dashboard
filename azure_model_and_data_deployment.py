import subprocess

subprocess.run("azure_db_model_deployment.py", shell=True)
subprocess.run("azure_data_import_to_db.py", shell=True)
subprocess.run("azure_create_views.py", shell=True)

print("""\nData model and data insert process has been completed.
Proces definisanja modela i ubacivanja podataka je završen.""")