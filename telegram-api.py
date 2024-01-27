import asyncio
from aiogram import Bot

async def get_channel_admins(api_token, channel_id):
    bot = Bot(token=api_token)
    # Fetch the chat information
    chat_info = await bot.get_chat(chat_id=channel_id)
    
    # Access the Chat object from the tuple
    chat = chat_info[0]
    
    # Access the User object for the creator
    creator = chat_info[1]
    
    print(f"Chat ID: {chat.id}, Title: {chat.title}")
    print(f"Creator ID: {creator.id}, Username: {creator.username}")

async def main():
    api_token = '6480409874:AAFvQY46c5x99drdA0jKGN86haKfhN-o5Gs'
    channel_id = -1001317428262  # Replace with the actual channel ID

    try:
        await get_channel_admins(api_token, channel_id)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
