class TableGetter:
    def get_table(self, table_name):
        return None


class ETL:
    def transform_table(self, table_getter):
        t = table_getter.get_table('employees')
        return t[t['a'] == 1]

    def calc_column(self, table_getter):
        t = table_getter.get_table('employees')
        t['sum'] = t['a'] + t['b']

        # This tactic is around 4 times slower! I'm guessing it's not vectorized?
        #t['sum'] = t.apply(lambda row: row.a + row.b, axis=1)
        return t
