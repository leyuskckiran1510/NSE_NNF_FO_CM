# Protocol For Non-NEAT Front End (NNF) - Python Implementation
### it has not been updated for more than 8months so please let me know what can be done,
### I am avilable for minor changes/bug fixes but anything bigger I wont be able to give time for it
### you can hire me at freelancing platform to continue my work on it for you personally 😉

Welcome to the Python implementation of the "Protocol For Non-NEAT Front End (NNF)" as described in the provided guidelines for programmers. This project aims to provide a Python library for working with the specified protocol, facilitating seamless communication between systems.

## Table of Contents
- [Overview](#overview)
- [Guidelines](#guidelines)
- [Data Types](#data-types)
- [Message Header](#message-header)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Overview

The "Protocol For Non-NEAT Front End (NNF)" defines a set of guidelines for programmers to establish communication between different systems. This Python implementation adheres to these guidelines, making it easier for developers to integrate the protocol into their applications.

## Guidelines

Please refer to the provided guidelines document for a detailed explanation of the rules and practices to be followed when working with this protocol. Pay special attention to aspects like byte order, data types, and structure alignment.

## Data Types

The protocol uses various data types, including CHAR, UINT, SHORT, LONG, LONG LONG, DOUBLE, and BIT. The size and signed/unsigned nature of each data type are outlined in the documentation.

## Message Header

The communication between systems is facilitated by a structured message header. The header includes essential information such as the transaction code, log time, user ID, error code, and timestamps. Refer to the documentation for a comprehensive understanding of the message header structure.

## Usage

To integrate this Python implementation into your project, follow these steps:

1. Install the package: `git clone https://github.com/leyuskckiran1510/NSE_NNF_FO_CM`
2. Import the library into your Python script.
3. Use the provided functions to build and parse messages according to the NNF protocol.

```python
# just make change to config file and you are good to go
```

## Contributing

Contributions to this project are welcome. Feel free to submit issues, feature requests, or pull requests. Follow the guidelines in the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

## License

This project is licensed under the [MIT License](LICENSE). See the [LICENSE](LICENSE) file for details.
