from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def excel_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("Oui", callback_data="excel_yes"),
            InlineKeyboardButton("Non", callback_data="excel_no")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def pagination_keyboard():
    keyboard = [
        [InlineKeyboardButton("Afficher plus", callback_data="more")]
    ]
    return InlineKeyboardMarkup(keyboard)