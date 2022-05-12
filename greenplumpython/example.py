from sum import pythonApply


def recsum(a, b):
    x = b
    return x


def avg_weather(id, city, p_date, temp, humidity, aqi):
    t = float(sum(temp)) / float(len(temp))
    t = format(t, ".2f")
    h = float(sum(humidity)) / float(len(humidity))
    h = format(h, ".2f")
    a = float(sum(aqi)) / float(len(aqi))
    a = format(a, ".2f")
    return (city, t, h, a)


if __name__ == "__main__":
    """
    print("===basic example===")
    input = ["a", "int4", "b", "int4"]
    output = ["a", "int8"]
    index = "a"
    pythonApply(input, output, index, recsum, "testtbl", "basic_output")
    """
    print("===weather forecast===")
    input = [
        "id",
        "int",
        "city",
        "text",
        "date",
        "timestamp",
        "temp",
        "int",
        "humidity",
        "int",
        "aqi",
        "int",
    ]
    output = ["city", "text", "avg_temp", "float", "avg_humidity", "float", "avg_aqi", "float"]
    index = "city"
    res = pythonApply(input, output, index, avg_weather, "input_data", "weather_output")
    # print(res)
