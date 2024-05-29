
import spark



import pytz

class tempCleanup:
    """
    docString
    """

    def __init__(self, catalog : str, schema : str):
        """
        docString
        """

        ## Initialize values
        self._tz = pytz.timezone("Europe/Copenhagen")
        self._catalog = catalog
        self._schema = schema
        self._historytable = "_delete_table"

        self._table = None
        self._description = None # Initialize with no description
        self._last_modified = None


    
    @property
    def table(self) -> str:
        return self._table
    @property
    def description(self) -> str:
        return self._description
    
    ## Return settings
    @property
    def tz(self):
        return self._tz
    @property
    def historytable(self) -> str:
        return self._historytable
    
    
    
    @table.setter
    def table(self, table : str) -> None:
        self._table = table
        self._description = self._get_table_description(table)
        self._last_modified = self.get_last_modified(table)

    ## Change options of time zone and name of "history table"
    @tz.setter
    def tz(self, zone : str) -> None:
        self._tz = pytz.timezone(zone)

    @historytable.setter
    def historytable(self, historytable : str) -> None:
        self._historytable = historytable

    
    def get_tables(self) -> list:
        """
        Retrieve all tables in the specified catalog and schema, excluding the history table.
        
        Returns:
            list: List of table names.
        """
        ## test
        return ["table1", "table2"]
    
        all_tables = spark.sql(f"SHOW TABLES IN {self._catalog}.{self._schema}")

        # Disregard the history table
        return [table for table in all_tables if table != {self._historytable}]
    




    def _get_table_description(self, table : str):
        """
        Retrieve the description of the specified table.
        
        Args:
            table (str): The table name.
        
        Returns:
            list: List of table descriptions.
        """
        ## test
        return f"{self._catalog}.{self._schema}.{table} description"
    
        return spark.sql(f"DESCRIBE TABLE {self._catalog}.{self._schema}.{table}").collect()
        
    def _get_last_modified(self, table : str):
        """
        
        """




    
tc = tempCleanup(catalog = "mycatalog", schema = "myschema")

tc.table = "hej"

tc.description


tc.set_description("hej")


print(tc._description)

print(tc.tz)

tc.tz = "UTC"
print(tc.tz)
