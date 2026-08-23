"""config_flow.py of an iClick Gateway."""
import ipaddress
import voluptuous as vol
import aiohttp
import json
import logging

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from .const import (
    DOMAIN, CONF_HOST, CONF_PORT, DEFAULT_PORT,
    ERROR_INVALID_IP, ERROR_CONNECTION_FAILED, ERROR_AUTH_FAILED, ERROR_DEVICE_DATA,
    CONF_ACCOUNT, CONF_PASSWORD, CONF_MAC, CONF_AREA, API_URL, DATA_DEVICE_DATA, DATA_IP_INFO
)

_LOGGER = logging.getLogger(__name__)

class BestjoyLoginConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_LOCAL_POLL
    
    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        """处理用户首次配置（显示表单）"""
        # 延迟导入避免阻塞事件循环
        from .client import BestjoyClient
        # 存储验证错误信息
        errors = {}
        
        if user_input is not None:
            try:
                # 验证hub_id范围及唯一性
                # hub_id = user_input["hub_id"]
                # if not (1 <= hub_id <=9):
                #    errors["base"] = "invalid_hub_id"
                if user_input[CONF_MAC] is None:
                      errors["base"] = "invalid_hub_id"
                else:
                    existing_hub_ids = [entry.data[CONF_MAC] for entry in self._async_current_entries()]
                    if user_input[CONF_MAC] in existing_hub_ids:
                        errors["base"] = "hub_id_already_used"
                    else:
                        # 从云端获取设备数据
                        device_data = await self._get_device_data(
                            user_input[CONF_ACCOUNT],
                            user_input[CONF_PASSWORD],
                            user_input[CONF_MAC]
                        )

                        if not device_data:
                            errors["base"] = ERROR_DEVICE_DATA
                        else:
                            devices = device_data.get("devices")
                            input_mac_ip_info = device_data.get(DATA_IP_INFO)
                            if not isinstance(devices, list) or not isinstance(input_mac_ip_info, dict):
                                _LOGGER.error(
                                    "iCLICK API result missing devices/ip_info: %s",
                                    device_data,
                                )
                                errors["base"] = ERROR_DEVICE_DATA
                            elif not input_mac_ip_info.get("ip"):
                                _LOGGER.error(
                                    "iCLICK API ip_info missing ip: %s",
                                    input_mac_ip_info,
                                )
                                errors["base"] = ERROR_DEVICE_DATA
                            else:
                                # 将设备数据存储在user_input中
                                user_input[DATA_DEVICE_DATA] = devices
                                # 初始化目标ip信息  {"mac": "", "ip":"", "room_name":""}
                                user_input[CONF_HOST] = input_mac_ip_info["ip"]
                                user_input[CONF_PORT] = DEFAULT_PORT
                                # 使用飞碟所在房间
                                user_input[CONF_AREA] = input_mac_ip_info.get(
                                    "room_name", ""
                                )

                                # 测试TCP连接
                                client = BestjoyClient(
                                    user_input[CONF_HOST],
                                    user_input[CONF_PORT],
                                    user_input[CONF_MAC]
                                )

                                if await client.async_test_connection():
                                    return self.async_create_entry(
                                        title=f"iCLICK_Hub_{user_input[CONF_MAC]}@{user_input[CONF_HOST]}",
                                        data=user_input
                                    )
                                errors["base"] = ERROR_CONNECTION_FAILED
            except ValueError:
                errors["base"] = ERROR_INVALID_IP
            except Exception as e:
                _LOGGER.error(f"Authentication failed: {str(e)}")
                errors["base"] = ERROR_AUTH_FAILED

        # 表单字段定义（包含新字段）
        data_schema = vol.Schema({
#             vol.Required(CONF_HOST): str,
#             vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
#             vol.Required("hub_id", default=1): vol.All(vol.Coerce(int), vol.Range(min=1, max=9)),
            vol.Required(CONF_ACCOUNT): str,
            vol.Required(CONF_PASSWORD): str,
            vol.Required(CONF_MAC): str,
        })
        
        return self.async_show_form(
            step_id="user", 
            data_schema=data_schema, 
            errors=errors,
            description_placeholders={
#                 CONF_HOST: "iClick网关的IP地址",
#                 CONF_PORT: "iClick网关的端口号（默认：8080）",
#                 "hub_id": "iClick网关的ID（1-9之间的数字）",
                CONF_ACCOUNT: "iCLICK账户用户名",
                CONF_PASSWORD: "iCLICK账户密码",
                CONF_MAC: "iCLICK网关的MAC地址"
            }
        )
    
    async def _get_device_data(self, account: str, password: str, mac: str) -> dict:
        """从云端API获取设备数据"""
        payload = {
            "jsonrpc": "2.0",
            "method": "getdata_v2",
            "params": [account, password, mac],
            "id": 1,
        }

        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(API_URL, json=payload) as response:
                    body = await response.text()
                    if response.status != 200:
                        _LOGGER.error(
                            "iCLICK API request failed with status %s: %s",
                            response.status,
                            body[:500],
                        )
                        return {}

                    if not body or not body.strip():
                        _LOGGER.error(
                            "iCLICK API returned empty body (status %s)",
                            response.status,
                        )
                        return {}

                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError as err:
                        _LOGGER.error(
                            "iCLICK API returned invalid JSON: %s; body=%s",
                            err,
                            body[:500],
                        )
                        return {}

                    # 云端偶发返回 JSON null，或 result 显式为 null
                    if not isinstance(data, dict):
                        _LOGGER.error(
                            "iCLICK API response is not an object: %r",
                            data,
                        )
                        return {}

                    if data.get("error"):
                        _LOGGER.error("iCLICK API returned error: %s", data["error"])
                        return {}

                    result = data.get("result")
                    if not isinstance(result, dict):
                        _LOGGER.error(
                            "iCLICK API missing/invalid result: %r (full=%s)",
                            result,
                            body[:500],
                        )
                        return {}

                    _LOGGER.debug("iCLICK API getdata_v2 result keys: %s", list(result))
                    return result
            except Exception as e:
                _LOGGER.error("iCLICK API request exception: %s", e)

        return {}
