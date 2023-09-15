# Databricks notebook source
# MAGIC %md
# MAGIC #Using Databricks Notebooks
# MAGIC 
# MAGIC [Learn Databricks Notebooks](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebooks-code)
# MAGIC 
# MAGIC [Databricks Notebook User Interface](https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-ui)
# MAGIC 
# MAGIC Table of contents in upper left corner of workspace show you contents if you use markdown cells as a way to group related cells.
# MAGIC 
# MAGIC There is a variable explorer to the right of the workspace cells.

# COMMAND ----------

# MAGIC %md
# MAGIC #Use Help to get list of keyboard short cuts
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/notebooks/toolbar.png">
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##Some of the most commonly used shortcuts are:
# MAGIC 
# MAGIC ###Execute Code:
# MAGIC 
# MAGIC Shift+Alt+Enter	:	Run all commands
# MAGIC 
# MAGIC Shift+Ctrl+Enter:  Executes the highlighted lines of code in a cell
# MAGIC 
# MAGIC Ctrl+Enter:        Executes the current cell and cursor remains in cell
# MAGIC 
# MAGIC Shift+Enter:       Executes the current cell and moves to next cell below.
# MAGIC 
# MAGIC 
# MAGIC ###Cell Manipulation
# MAGIC 
# MAGIC Alt+Up/Down	:	Move to previous/next cell
# MAGIC 
# MAGIC Ctrl+Alt+P	:	Insert a cell above
# MAGIC 
# MAGIC Ctrl+Alt+N	:	Insert a cell below
# MAGIC 
# MAGIC Ctrl+Alt+-	:	Split a cell at cursor
# MAGIC 
# MAGIC Alt+^       :   Move up cell
# MAGIC 
# MAGIC Alt+v       :   Move down cell

# COMMAND ----------

# MAGIC %md
# MAGIC #Set Some Variables and click on variable explorer to the right {x}

# COMMAND ----------

#execute this cell to set variables

onetwo = 12

thisstring = "this string"


# COMMAND ----------

# MAGIC %md
# MAGIC #Magic Commands
# MAGIC Magic commands will allow you to override the default language for a notebook for the current cell.
# MAGIC 
# MAGIC %md is the magic command to create a markup cell typically used for documenting your notebook.
# MAGIC 
# MAGIC %sql is the magic command to use Databricks SQL for a cell
# MAGIC 
# MAGIC %r is the magic command to use the R language for a cell
# MAGIC 
# MAGIC %python is the magic command to use the Pythone language for a cel.
# MAGIC 
# MAGIC ## Note that at the top of a notebook there is a default language.

# COMMAND ----------

# MAGIC %md
# MAGIC #Use Revision History to see your changes as you edit notebooks.
# MAGIC 
# MAGIC ## Github Repositories are well integated into Azure Databricks Notebooks.
# MAGIC 
# MAGIC [Integrate Github with Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/repos/git-version-control-legacy)
# MAGIC 
# MAGIC [Create Personal Access Token to Link your Repository to Databricks](https://learn.microsoft.com/en-us/azure/databricks/repos/get-access-tokens-from-git-provider)
