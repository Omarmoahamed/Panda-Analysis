
import Analysis_Summary


class EngineFactory:
   
    Engine_registry = {}


    @staticmethod
    def get_engine(engine_type, *args, **kwargs)-> Analysis_Summary:
        
        try:
            cls = EngineFactory.Engine_registry[engine_type]
            return cls(*args, **kwargs)
        except KeyError:
            raise ValueError(f"Unknown engine type: {engine_type}")

        
        
    def register_engine(name):
     def wrapper(cls):
        EngineFactory.ENGINE_REGISTRY[name] = cls
        return cls
     return wrapper        