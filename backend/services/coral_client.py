import json
import asyncio
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

async def query_coral(sql: str) -> List[Dict[str, Any]]:
    """
    Executes a SQL query against Coral CLI and returns the parsed JSON results.
    This is a lightweight integration step to get started with Coral without full MCP overhead.
    """
    try:
        # We use asyncio.create_subprocess_exec to run the CLI command non-blocking
        process = await asyncio.create_subprocess_exec(
            "coral", "sql", sql, "--format", "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            logger.error(f"Coral query failed: {error_msg}")
            raise RuntimeError(f"Coral query failed: {error_msg}")
            
        output = stdout.decode().strip()
        if not output:
            return []
            
        return json.loads(output)
        
    except Exception as e:
        logger.error(f"Error executing Coral query: {e}")
        raise
