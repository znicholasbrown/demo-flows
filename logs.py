from prefect import flow, get_run_logger, task

def rainbow_wave(text):
    colors = [31, 33, 32, 36, 34, 35, 35]  # ANSI color codes for ROYGBIV
    rainbow = ""
    for i, char in enumerate(text):
        if char.isspace():
            rainbow += char
        else:
            color = colors[i % len(colors)]
            rainbow += f"\u001b[{color}m{char}\u001b[0m"
    return rainbow

lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."

cases = [
    "\u001b[0mReset (0) - Text Formatting\u001b[0m",
    "\u001b[1mBold or Increased Intensity (1) - Text Formatting\u001b[0m",
    "\u001b[2mFaint or Decreased Intensity (2) - Text Formatting\u001b[0m",
    "\u001b[3mItalic (3) - Text Formatting\u001b[0m",
    "\u001b[4mUnderline (4) - Text Formatting\u001b[0m",
    "\u001b[5mSlow Blink (5) - Text Formatting\u001b[0m",
    "\u001b[6mRapid Blink (6) - Text Formatting\u001b[0m",
    "\u001b[7mReverse Video or Invert (7) - Text Formatting\u001b[0m",
    "\u001b[8mConceal or Hide (8) - Text Formatting\u001b[0m",
    "\u001b[9mCrossed-out or Strike (9) - Text Formatting\u001b[0m",
    "\n",
    "\u001b[30mBlack Foreground (30) - Standard Foreground Color\u001b[0m",
    "\u001b[31mRed Foreground (31) - Standard Foreground Color\u001b[0m",
    "\u001b[32mGreen Foreground (32) - Standard Foreground Color\u001b[0m",
    "\u001b[33mYellow Foreground (33) - Standard Foreground Color\u001b[0m",
    "\u001b[34mBlue Foreground (34) - Standard Foreground Color\u001b[0m",
    "\u001b[35mMagenta Foreground (35) - Standard Foreground Color\u001b[0m",
    "\u001b[36mCyan Foreground (36) - Standard Foreground Color\u001b[0m",
    "\u001b[37mWhite Foreground (37) - Standard Foreground Color\u001b[0m",
    "\n",
    "\u001b[90mBright Black Foreground (90) - Bright Foreground Color\u001b[0m",
    "\u001b[91mBright Red Foreground (91) - Bright Foreground Color\u001b[0m",
    "\u001b[92mBright Green Foreground (92) - Bright Foreground Color\u001b[0m",
    "\u001b[93mBright Yellow Foreground (93) - Bright Foreground Color\u001b[0m",
    "\u001b[94mBright Blue Foreground (94) - Bright Foreground Color\u001b[0m",
    "\u001b[95mBright Magenta Foreground (95) - Bright Foreground Color\u001b[0m",
    "\u001b[96mBright Cyan Foreground (96) - Bright Foreground Color\u001b[0m",
    "\u001b[97mBright White Foreground (97) - Bright Foreground Color\u001b[0m",
    "\n",
    "\u001b[40mBlack Background (40) - Standard Background Color\u001b[0m",
    "\u001b[41mRed Background (41) - Standard Background Color\u001b[0m",
    "\u001b[42mGreen Background (42) - Standard Background Color\u001b[0m",
    "\u001b[43mYellow Background (43) - Standard Background Color\u001b[0m",
    "\u001b[44mBlue Background (44) - Standard Background Color\u001b[0m",
    "\u001b[45mMagenta Background (45) - Standard Background Color\u001b[0m",
    "\u001b[46mCyan Background (46) - Standard Background Color\u001b[0m",
    "\u001b[47mWhite Background (47) - Standard Background Color\u001b[0m",
    "\n",
    "\u001b[100mBright Black Background (100) - Bright Background Color\u001b[0m",
    "\u001b[101mBright Red Background (101) - Bright Background Color\u001b[0m",
    "\u001b[102mBright Green Background (102) - Bright Background Color\u001b[0m",
    "\u001b[103mBright Yellow Background (103) - Bright Background Color\u001b[0m",
    "\u001b[104mBright Blue Background (104) - Bright Background Color\u001b[0m",
    "\u001b[105mBright Magenta Background (105) - Bright Background Color\u001b[0m",
    "\u001b[106mBright Cyan Background (106) - Bright Background Color\u001b[0m",
    "\u001b[107mBright White Background (107) - Bright Background Color\u001b[0m",
]

@task
def palette_test_cases():
    logger = get_run_logger()
    for case in cases:
        logger.info(case)


@flow
def logs():
    logger = get_run_logger()

    # Basic colors
    logger.debug("\u001b[31mRed\u001b[0m \u001b[32mGreen\u001b[0m \u001b[34mBlue\u001b[0m")

    # Nested colors
    logger.info("\u001b[31mRed \u001b[32mGreen \u001b[34mBlue\u001b[32m Green\u001b[31m Red\u001b[0m")

    # Multiple styles
    logger.warning("\u001b[1m\u001b[31mBold Red\u001b[0m \u001b[4m\u001b[34mUnderlined Blue\u001b[0m")

    # Background colors
    logger.error("\u001b[41m\u001b[37mWhite on Red\u001b[0m \u001b[44m\u001b[30mBlack on Blue\u001b[0m")

    # Mixed content with URL
    logger.info("Check out this \u001b[32mgreen link\u001b[0m: https://example.com")

    # Bright colors
    logger.debug("\u001b[91mBright Red\u001b[0m \u001b[96mBright Cyan\u001b[0m")

    # Color in the middle of a word
    logger.warning("Colo\u001b[31mr\u001b[0mful text")

    # Multiple resets
    logger.error("\u001b[31mRed\u001b[0m\u001b[0m\u001b[0m Normal")

    # Overlapping styles
    logger.critical("\u001b[1m\u001b[31m\u001b[4mBold Red Underline\u001b[24m Just Bold Red\u001b[0m")

    # All text styles
    logger.info("\u001b[1mBold\u001b[0m \u001b[2mDim\u001b[0m \u001b[3mItalic\u001b[0m \u001b[4mUnderline\u001b[0m \u001b[9mStrikethrough\u001b[0m")

    # Complex nested structure with link
    logger.debug("\u001b[31mError in module: \u001b[0m\u001b[1m\u001b[34mauth.py\u001b[0m\u001b[31m. See logs at: \u001b[0m\u001b[4mhttps://logs.example.com/auth\u001b[0m")

    # Mixing bright and normal colors
    logger.warning("\u001b[31mNormal Red\u001b[91m Bright Red\u001b[33m Normal Yellow\u001b[93m Bright Yellow\u001b[0m")

    # Color codes without printable text in between
    logger.info("\u001b[31m\u001b[32m\u001b[34mColored\u001b[0m")

    # Invalid/unsupported color codes
    logger.debug("\u001b[99mInvalid\u001b[0m \u001b[31mValid\u001b[0m")

    # Emojis with colors
    logger.info("\u001b[31m🔴 Red Circle\u001b[0m \u001b[34m🔵 Blue Circle\u001b[0m")

    # Long text with intermittent colors
    logger.warning("This is a \u001b[93mlong\u001b[0m warning message that \u001b[93mcontains\u001b[0m multiple \u001b[93mcolored\u001b[0m words throughout its \u001b[93mcontent\u001b[0m.")

    logger.debug(rainbow_wave(lorem_ipsum))

    palette_test_cases()

if __name__ == "__main__":
    logs()