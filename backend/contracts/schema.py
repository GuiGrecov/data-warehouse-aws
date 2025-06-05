from typing import Union, Dict 

GenericSchema = Dict[str, Union[str, int, float, bool, None]]

CompraSchema: GenericSchema = {
    "ean":str,
    "price":float,
    "store":int,
    "dateTime":str

}