import asyncio
import logging
from pathlib import Path

import aiofiles
from aiohttp import web
from environs import Env

env = Env()
env.read_env()


CHUNK_SIZE_BYTES = 100 * 1024

PHOTOS_ROOT_DIR = Path(env('PHOTOS_ROOT_DIR_PATH'))
IS_LOGGING_ENABLED = env.bool('IS_LOGGING_ENABLED', True)
RESPONSE_LAG = env.int('RESPONSE_LAG', 0)


async def archive_handler(request: web.Request):
    """Collects the archive and sends it to the user."""
    response = web.StreamResponse()
    photos_hash = request.match_info.get('archive_hash')

    photos_path = PHOTOS_ROOT_DIR / photos_hash
    if not photos_path.exists():
        raise web.HTTPNotFound(text=f'The archive {photos_hash} does not exist!')

    filename = f'photos_{photos_hash}.zip'
    response.headers['Content-Disposition'] = f'Attachment;filename={filename}'

    await response.prepare(request)

    process = await asyncio.create_subprocess_exec(
        'zip',
        '-r',
        '-',
        '.',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=CHUNK_SIZE_BYTES,
        cwd=photos_path,
    )
    try:
        while True:
            chunk = await process.stdout.read(n=CHUNK_SIZE_BYTES)
            logging.info('Getting archive chunk ...')
            if process.stdout.at_eof():
                break
            await response.write(chunk)
            await asyncio.sleep(RESPONSE_LAG)
    except asyncio.CancelledError:
        logging.info('Download was interrupted')
    except KeyboardInterrupt:
        logging.info('hello!')
    finally:
        process.terminate()
    return response


async def index_page_handler(request: web.Request):
    """Show static index page."""
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(name)-15s %(pathname)-130s %(funcName)-30s %(message)s',
        level=logging.INFO,
    )
    if not IS_LOGGING_ENABLED:
        logging.disable(logging.CRITICAL)
    app = web.Application()
    app.add_routes([
        web.get('/', index_page_handler),
        web.get('/archive/{archive_hash}/', archive_handler),
    ])
    web.run_app(app)
