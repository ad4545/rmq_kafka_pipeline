package utils

import (
	"rmqkafka_pipeline/pubsub/iface"
)

// type GeneratorState struct {
// 	//map of package name to its Messages: pkg -> [msgName -> MessageDefinition]
// 	RosMsgPkgs map[string]map[string]interface{}
// 	// map of ros messages and their proto paths
// 	ProtoPkgPath map[string]string
// 	// map of ros messages and their generared proto paths
// 	ProtoImportPath map[string]string
// }

var builders *builderUniverse

type BuilderFinder struct {
	GetBuilderFromName func(string) (iface.Builder, error)
	Generated          bool
}

func (b BuilderFinder) IsGenerated() bool {
	return b.Generated
}

type builderUniverse struct {
	builders map[string]BuilderFinder
}

func NewBuilder(name string, finder func(string) (iface.Builder, error)) BuilderFinder {

	if builders == nil {
		builders = &builderUniverse{}
		builders.builders = map[string]BuilderFinder{}
	}

	if _, ok := builders.builders[name]; ok {
		if builders.builders[name].GetBuilderFromName == nil && finder != nil {
			builders.builders[name] = BuilderFinder{GetBuilderFromName: finder, Generated: true}
		}
	} else {
		if finder != nil {
			builders.builders[name] = BuilderFinder{GetBuilderFromName: finder, Generated: true}
		} else {
			builders.builders[name] = BuilderFinder{Generated: false}
		}
	}

	return builders.builders[name]

}
