namespace Raven.Quill.AiHelper;

public static class AiHelperStatusExtensions
{
    /// <summary>
    /// Whether the AI service answered at all, as opposed to being unreachable or broken. The
    /// distinction decides who the operator should go and fix: a status the service reported is
    /// theirs to act on, anything else is the service's own problem and reads as a 502.
    /// </summary>
    public static bool ServiceAnswered(this AiHelperStatus status) =>
        status is AiHelperStatus.Success or AiHelperStatus.ConsentRequired or AiHelperStatus.InvalidCredentials;
}
