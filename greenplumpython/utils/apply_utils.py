import random
import string

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_func"+''.join(random.choice(letters) for i in range(stringLength))

def randomStringType(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_type"+''.join(random.choice(letters) for i in range(stringLength))

def createTypeFunc(sig, typeName):
    typeSQL = ""
    for i  in range(0,len(sig)):
        if i == 0:
            for j, col in enumerate(sig[i]):
                typeSQL += col + " " + sig[i][col]
        else:
            for j, col in enumerate(sig[i]):
                typeSQL += ",\n" + col + " " + sig[i][col]
    typeSQL = "CREATE TYPE " + typeName + " AS (\n" + typeSQL + "\n);"
    return typeSQL