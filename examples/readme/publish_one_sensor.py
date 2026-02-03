import argparse
import rembus as rb


def main():
    parser = argparse.ArgumentParser(
        description="Publish a message to a Rembus topic")
    parser.add_argument(
        "-t", "--topic",
        default="mysite/HVAC/mysite.sensor3/sensor",
        help="Topic to publish to (default: %(default)s)"
    )
    parser.add_argument(
        "--url",
        default="ws://:8000/myclient",
        help="Rembus node URL (default: %(default)s)"
    )

    args = parser.parse_args()

    with rb.node(args.url) as cli:
        cli.publish(args.topic)


if __name__ == "__main__":
    main()
