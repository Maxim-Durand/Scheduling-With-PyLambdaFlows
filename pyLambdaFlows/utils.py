from .DynamoGesture import get_entries_group, get_entries_list

def isIterable(obj):
    try:
        _ = iter(obj)
    except TypeError:
        return False
    return True


class PromessResult():
    def __init__(self, table_name, result_idx, sess):
        self.result_idx = result_idx
        self.session = sess 
        self.table = table_name

    def getStatus(self):
        table_data = get_entries_group(self.table, min(self.result_idx), max(self.result_idx), sess=self.session)
        reveive_elements = {element[0] for element in table_data}
        forgoten_elements = set(self.result_idx).symmetric_difference(reveive_elements)
        while len(forgoten_elements)>0:
            batch_table = get_entries_list(self.table, list(forgoten_elements), sess=self.session)
            table_data.extend(batch_table)
            reveive_elements = {element[0] for element in table_data}
            forgoten_elements = set(self.result_idx).symmetric_difference(reveive_elements)
        table_data = list(sorted(table_data, key=lambda element : element[0]))

        remaining = list(filter(lambda  element: element[1] is None, table_data))
        done = list(filter(lambda  element: not element[1] is None, table_data))
        return [element[0] for element in remaining], [element[0] for element in done]

    def getResult(self):
        table_data =  get_entries_group(self.table, min(self.result_idx), max(self.result_idx), sess=self.session)
        reveive_elements = {element[0] for element in table_data}
        forgoten_elements = set(self.result_idx).symmetric_difference(reveive_elements)
        while len(forgoten_elements)>0:
            batch_table = get_entries_list(self.table, list(forgoten_elements), sess=self.session)
            table_data.extend(batch_table)
            reveive_elements = {element[0] for element in table_data}
            forgoten_elements = set(self.result_idx).symmetric_difference(reveive_elements)
        table_data = list(sorted(table_data, key=lambda element : element[0]))
        return [element[1] for element in table_data]