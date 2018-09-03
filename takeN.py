import sys

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("not enough arguments")
        print("usage: {} <movies to take> <path to dataset>".format(sys.argv[0]))
        exit()

    count = int(sys.argv[1])
    readElements = ""
    with open(sys.argv[2], 'r',  encoding='utf-8', errors='ignore') as f:
        while count > 0:
            line = f.readline()
            readElements += line

            if line == '\n':
                count -= 1

    with open("movies.txt", 'w') as f:
        f.write(readElements)
    