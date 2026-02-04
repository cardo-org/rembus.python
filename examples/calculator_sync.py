import rembus as rb


def add(x, y):
    return x + y


def main():
    srv = rb.node()
    srv.expose(add)
    srv.wait()


if __name__ == "__main__":
    main()
