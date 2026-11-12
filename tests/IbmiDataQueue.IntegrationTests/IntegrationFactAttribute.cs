using Xunit;

namespace IbmiDataQueue.IntegrationTests;

/// <summary>Comme [Fact], mais ignoré tant que les variables d'environnement IBMI_* ne sont pas définies.</summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    public IntegrationFactAttribute()
    {
        if (!IntegrationSettings.IsConfigured)
        {
            Skip = "Définir IBMI_HOST, IBMI_USER, IBMI_PASSWORD et IBMI_LIBRARY pour lancer les tests d'intégration IBM i.";
        }
    }
}
