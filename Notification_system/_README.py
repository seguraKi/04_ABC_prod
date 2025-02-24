# Databricks notebook source
# MAGIC %md
# MAGIC # Folder description
# MAGIC **Business context explanation:** Goal of this folder is to provide a standardized way of sending notification to OOQ users. Project manager has various requirements for the discovery of errors and notification logic. Each notebook fulfills particular need. Currently there are two types: duplicate and mismatch. 
# MAGIC
# MAGIC **Architecture**
# MAGIC >step (what executes the step)
# MAGIC
# MAGIC >transformations (function) -> message_content_creation (function) -> assemble_email (EmailingService) -> send_email (Emailing_Service)
# MAGIC
# MAGIC 1. There is standard table transformation involved. It is enclosed in `transformation(source, other_dfs)` function.
# MAGIC 2. The message is then created depending of the match with regex pattern. Again a function is called to provide different content depending on the match.
# MAGIC 3. Using EmailingService class from emailing_module we then send the email notification to the user.
# MAGIC
# MAGIC **Developers involved:** kubiesa.a.1@pg.com
# MAGIC
# MAGIC **Future changes:** Standardize mailing_list retrieval to comply with DRY rule -> Introduce new class inheriting from abstract BaseTable when it will be introduced.
# MAGIC Deadline reminders should be migrated here in the future to comply with SOLID principles and DRY rule.
