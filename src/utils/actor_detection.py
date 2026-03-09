def detect_actor(text):

    text = text.lower()

    user_keywords = [
        "unlock",
        "sent",
        "outgoing",
        "typed",
        "photo_taken",
        "camera",
        "message_sent"
    ]

    system_keywords = [
        "background",
        "sync",
        "update",
        "system",
        "scheduler",
        "play store"
    ]

    for k in user_keywords:
        if k in text:
            return "likely_user"

    for k in system_keywords:
        if k in text:
            return "likely_system"

    return "unknown"