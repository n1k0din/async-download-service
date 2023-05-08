import asyncio
import logging
from functools import partial
from pathlib import Path

import aiofiles
from aiohttp import web
from environs import Env


async def archive_handler(
    request: web.Request,
    photos_root_dir: Path,
    response_lag: int,
    chunk_size_bytes: int,
):
    """Collects the archive and sends it to the user."""
    response = web.StreamResponse()
    photos_hash = request.match_info['archive_hash']

    photos_path = photos_root_dir / photos_hash
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
        limit=chunk_size_bytes,
        cwd=photos_path,
    )
    try:
        while not process.stdout.at_eof():
            chunk = await process.stdout.read(n=chunk_size_bytes)
            logging.info('Getting archive chunk ...')
            await response.write(chunk)
            await asyncio.sleep(response_lag)
    except asyncio.CancelledError:
        logging.info('Download was interrupted')
        raise
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
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
    env = Env()
    env.read_env()

    photos_root_dir = Path(env('PHOTOS_ROOT_DIR_PATH'))
    is_logging_enabled = env.bool('IS_LOGGING_ENABLED', True)
    response_lag = env.int('RESPONSE_LAG', 0)
    chunk_size_bytes = env.int('CHUNK_SIZE_KBYTES', 100) * 1024

    if not is_logging_enabled:
        logging.disable(logging.CRITICAL)

    app = web.Application()
    app.add_routes([
        web.get('/', index_page_handler),
        web.get(
            '/archive/{archive_hash}/',
            partial(
                archive_handler,
                photos_root_dir=photos_root_dir,
                response_lag=response_lag,
                chunk_size_bytes=chunk_size_bytes,
            ),
        ),
    ])
    web.run_app(app)
