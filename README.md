

# Turbo

This maven plugin can be used in order to compare the data structure between the following sources:
- Database
- Hibernate definition files
- Migration sql files
- ERD diagram

Note:
- Although it is tailored to a specific database and ERD application, this plugin can still be adjusted for 
other database types or ERD applications respectively.
- Also the SQL statements are supposed to be in certain locations which is also adjustable.

If this plugin is activated, depending on the configuration, it creates formatted console outputs 
about all the differences in the configured sources.

It does also create an Excel document where each field and their properties can be compared 
between the specified sources.

