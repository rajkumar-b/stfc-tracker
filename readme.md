## STFC Activity Tracker

### Project Requirements
1. Install Python + PIP and any suitable IDE (PyCharm or VisualStudio).
2. Install an interpreter (Conda or Venv).
3. Install docker desktop (For local MySQL). Ignore if you have other means to connect to an external MySQL DB.

### Project Execution.
1. Open project in IDE. Set up interpreter for the project.
2. Install the pip requirements for the project, either via IDE or use `pip install -r requirements.txt`
3. Create database and table for MySQL (refer mysql_requirements.txt) under your instance. 
Docker might come in handy if you have used it earlier, in which case, install mysql image first,
and run `docker run -p 3306:3306 -p 33060:33060 --name test-mysql -e MYSQL_ROOT_PASSWORD=abc123 mysql` to create db.
4. Go to lambda_function.py, edit the initial MySQL configs (at top) properly and run the lambda handler
(from last line).
