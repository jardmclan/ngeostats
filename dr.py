with open("all_data") as f:
    i = 0
    for line in f:
        print(line)
        if i > 3:
            break
        i = i + 1