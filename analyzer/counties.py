import csv


def get_county_to_state_map():
    m = {}

    # Make map from counties to state
    with open('counties.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for line in reader:
            m[line[0]] = line[1]

    return m
