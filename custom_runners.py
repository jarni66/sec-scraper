from copy import deepcopy
import json
from agents import Agent,  Runner, ModelSettings, WebSearchTool, Usage, RunContextWrapper, RunHooks
import uuid
import re
import asyncio
from dotenv import load_dotenv
import os
from openai import AsyncOpenAI
from agents import Agent, Runner, OpenAIChatCompletionsModel, RunConfig, set_tracing_disabled
load_dotenv() # Load environment variables from .env file
import copy 


set_tracing_disabled(True)

class UsageHook(RunHooks):
    def __init__(self):
        self.usage_data = {
            "model" : "gpt-4o-mini",
            "model_name" : "",
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "requests": 0
        }

    async def on_agent_end(self, context: RunContextWrapper, agent: Agent, output: str) -> None:
        usage: Usage = context.usage
        self.usage_data['model'] = agent.model
        self.usage_data['model_name'] = agent.name
        self.usage_data['input_tokens'] = usage.input_tokens
        self.usage_data['output_tokens'] = usage.output_tokens
        self.usage_data['total_tokens'] = usage.total_tokens
        self.usage_data['requests'] = usage.requests
        

class CRunner:
    def __init__(
        self,
        agent : Agent,
        prompt,
        format_output = None,
        model = "gpt-4o-mini",
        tools = [],
        running_debug = True
        ):
        # self.agent = deepcopy(agent)
        self.agent = agent
        # self.agent = copy.copy(agent)
        self.prompt = prompt
        self.format_output = format_output
        self.model = model
        self.tools = tools
        self.output = ""
        self.agent_id = self._unique_agent_id(agent.name)
        self.hook = UsageHook()
        self.usage = {"web_search":0}
        self.running_debug = running_debug
        
        
        
    def _unique_agent_id(self, name: str) -> str:
        # Convert to lowercase, replace spaces with underscores
        slug = re.sub(r'\W+', '_', name.lower()).strip('_')
        
        # Add short UUID (first 4–6 chars)
        short_uuid = uuid.uuid4().hex[:4]
        
        return f"{slug}_{short_uuid}"
        
    async def _run(self):
        if self.running_debug:
            print("Running :", self.agent.name )
        
        if self.tools:
            updated_tools = self.agent.tools + self.tools
            # self.agent.tools = updated_tools

            unique_tools = {tool.name: tool for tool in updated_tools}.values()
            # self.agent.tools = self.tools
            
        # if self.agent.tools:
        #     self.agent.model_settings = ModelSettings(temperature=0.1, tool_choice="required")
        
            
        if self.format_output:
                
            self.agent.output_type = self.format_output
            
            if self.agent.name.startswith('gemini'):
                result = await Runner.run(
                    self.agent, 
                    self.prompt,
                    run_config=agent_run_config
                    )
                
            else:
                result = await Runner.run(
                    self.agent, 
                    self.prompt,
                    hooks=self.hook)

            structured_output = result.final_output  

            json_res = structured_output.json()
            if type(json_res) == str:
                json_data = json.loads(json_res)
                self.output = json_data
            else:
                self.output = json_res
        else:
            if self.agent.name.startswith('gemini'):
                result = await Runner.run(
                    self.agent, 
                    self.prompt,
                    run_config=agent_run_config
                    )
                
            else:
                result = await Runner.run(
                    self.agent, 
                    self.prompt,
                    hooks=self.hook)
            
            self.output = result.final_output
    
    async def run_async(self):
        try:
            return await self._run()
        except:
            try:
                loop = asyncio.get_running_loop()
                # asyncio.set_event_loop(loop)
                task = loop.create_task(self._run())
                return await asyncio.gather(task)
            
            except:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                # return loop.run_until_complete(self._run())
                return await self._run()
    
    def run(self):
        
        # asyncio.run(self._run())
        try:
            loop = asyncio.get_running_loop()
            # If running in an existing loop, schedule a task
            task = loop.create_task(self._run())
            return task
        except RuntimeError as e:
            # Case: no running loop or it's already closed
            try:
                return asyncio.run(self._run())
            except RuntimeError as e2:
                if "event loop is closed" in str(e2):
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    # return loop.run_until_complete(self._run())
                    return loop.run_until_complete(self._run())
                raise
        
        # self.usage.update(self.hook.usage_data)
        # for t in self.agent.tools:
        #     if t.name == 'web_search_preview':
        #         self.usage['web_search'] += 1