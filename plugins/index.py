import logging
import asyncio
import re, time
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, ChannelInvalid
from pyrogram.errors.exceptions.bad_request_400 import UsernameInvalid, UsernameNotModified, UserIsBlocked
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from info import ADMINS, LOG_CHANNEL, INDEX_EXTENSIONS
from database.ia_filterdb import save_file
from utils import temp, get_readable_time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

lock = asyncio.Lock()

@Client.on_callback_query(filters.regex(r'^index'))
async def index_files(bot, query):
    _, ident, chat, lst_msg_id, skip = query.data.split("#")
    logger.info(f"Indexing requested with ident: {ident}, chat: {chat}, lst_msg_id: {lst_msg_id}, skip: {skip}")
    if ident == 'yes':
        msg = query.message
        await msg.edit("Starting Indexing...")
        try:
            chat = int(chat)
        except ValueError:
            chat = chat
        await index_files_to_db(int(lst_msg_id), chat, msg, bot, int(skip))
    elif ident == 'cancel':
        temp.CANCEL = True
        await query.message.edit("Trying to cancel Indexing...")
        logger.info("Indexing cancellation requested.")

@Client.on_message(
    (filters.forwarded | (filters.regex("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")) & filters.text) 
    & filters.private & filters.incoming)
async def send_for_index(bot, message):
    logger.info(f"Received indexing request from user: {message.from_user.id} with message: {message.text}")
    if message.text:
        regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
        match = regex.match(message.text)
        if not match:
            return await message.reply('Invalid link')
        chat_id = match.group(4)
        last_msg_id = int(match.group(5))
        if chat_id.isnumeric():
            chat_id = int("-100" + chat_id)
    elif message.forward_from_chat and message.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = message.forward_from_message_id
        chat_id = message.forward_from_chat.username or message.forward_from_chat.id
    else:
        return

    try:
        await bot.get_chat(chat_id)
    except ChannelInvalid:
        logger.warning(f"Channel invalid or bot is not an admin in chat ID: {chat_id}")
        return await message.reply('This may be a private channel/group. Make me an admin over there to index the files.')
    except (UsernameInvalid, UsernameNotModified):
        logger.error("Invalid username provided in the link")
        return await message.reply('Invalid Link specified.')
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return await message.reply(f'Errors - {e}')

    try:
        k = await bot.get_messages(chat_id, last_msg_id)
    except FloodWait as e:
        logger.warning(f"FloodWait triggered for {e.x} seconds.")
        await asyncio.sleep(e.x)
        return await message.reply('Please wait due to server limitations.')
    except Exception as e:
        logger.exception("Error fetching messages from chat")
        return await message.reply('Ensure that I am an admin in the channel if it is private.')

    if k.empty:
        return await message.reply('This may be a group, and I am not an admin of the group.')

    await message.reply_text(
        text="<b>Send the skip message number.\n\nIf you don’t want to skip any files, send 0.</b>",
        reply_to_message_id=message.id,
        reply_markup=ForceReply(True)
    )

@Client.on_message(filters.private & filters.reply)
async def forceskip(client, message):
    reply_message = message.reply_to_message
    if (reply_message.reply_markup) and isinstance(reply_message.reply_markup, ForceReply):
        try:
            skip = int(message.text)
        except ValueError:
            await message.reply("Invalid number provided; using 0 as a skip number.")
            skip = 0

        msg = await client.get_messages(message.chat.id, reply_message.id)
        info = msg.reply_to_message
        if info.text:
            regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
            match = regex.match(info.text)
            if not match:
                return await info.reply('Invalid link')
            chat_id = match.group(4)
            last_msg_id = int(match.group(5))
            if chat_id.isnumeric():
                chat_id = int("-100" + chat_id)
        elif info.forward_from_chat and info.forward_from_chat.type == enums.ChatType.CHANNEL:
            last_msg_id = info.forward_from_message_id
            chat_id = info.forward_from_chat.username or info.forward_from_chat.id
        else:
            return

        await message.delete()
        await msg.delete()
        
        if message.from_user.id in ADMINS:
            buttons = [
                [InlineKeyboardButton('Yes', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}')],
                [InlineKeyboardButton('Close', callback_data='close_data')]
            ]
            reply_markup = InlineKeyboardMarkup(buttons)
            await message.reply(
                f'Do you want to index this channel/group?\n\nChat ID/Username: <code>{chat_id}</code>\nLast Message ID: <code>{last_msg_id}</code>',
                reply_markup=reply_markup
            )

async def index_files_to_db(lst_msg_id, chat, msg, bot, skip):
    start_time = time.time()
    total_files = duplicate = errors = deleted = no_media = unsupported = 0
    current = skip
    logger.info(f"Starting indexing process in chat: {chat} from message ID: {lst_msg_id} with skip: {skip}")

    async with lock:
        try:
            async for message in bot.iter_messages(chat, lst_msg_id, skip):
                if temp.CANCEL:
                    temp.CANCEL = False
                    time_taken = get_readable_time(time.time() - start_time)
                    await msg.edit(f"Indexing canceled. Completed in {time_taken}.")
                    return
                
                if current % 30 == 0:
                    btn = [[InlineKeyboardButton('CANCEL', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]
                    await msg.edit_text(
                        text=f"Processed messages: {current}, Saved: {total_files}, Duplicates: {duplicate}, Deleted: {deleted}, "
                             f"Non-media: {no_media + unsupported}, Errors: {errors}",
                        reply_markup=InlineKeyboardMarkup(btn)
                    )
                
                current += 1
                if message.empty:
                    deleted += 1
                    continue
                elif not message.media:
                    no_media += 1
                    continue
                elif message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.DOCUMENT]:
                    unsupported += 1
                    continue
                
                media = getattr(message, message.media.value, None)
                if not media:
                    unsupported += 1
                    continue
                elif not str(media.file_name).lower().endswith(tuple(INDEX_EXTENSIONS)):
                    unsupported += 1
                    continue
                
                media.caption = message.caption
                status = await save_file(media)
                if status == 'suc':
                    total_files += 1
                elif status == 'dup':
                    duplicate += 1
                elif status == 'err':
                    errors += 1
        except Exception as e:
            logger.error(f"Error during indexing: {e}")
            await msg.reply(f'Indexing canceled due to error - {e}')
        else:
            time_taken = get_readable_time(time.time() - start_time)
            await msg.edit(f'Successfully saved <code>{total_files}</code> files to the database. Completed in {time_taken}.')
